/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import java.lang.reflect.Modifier

import scala.jdk.CollectionConverters._

import com.google.common.reflect.ClassPath

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.util.Utils

/**
 * Guard suite for the stateful-expression contract (see SPARK-58154).
 *
 * An [[Expression]] that keeps mutable state in an instance field must override
 * [[Expression.stateful]] to return `true`. Otherwise
 * [[Expression.freshCopyIfContainsStatefulExpression]] will not make a private copy of the
 * expression before evaluation, so a single shared instance evaluated concurrently across threads
 * (or reused for several output columns) can corrupt that state. The parent ticket SPARK-58154
 * fixed a batch of such expressions one by one; this suite is the regression net that keeps new
 * ones from slipping in unmarked.
 *
 * Detection targets the exact shape those bugs took: a mutable `@transient` instance field -- a
 * per-task scratch buffer that is (re)initialized on the executor and written during evaluation,
 * and is `@transient` precisely because it must not be serialized with the expression. This is a
 * deliberately narrow signal:
 *   - it catches every expression fixed under SPARK-58154 (all of which use `@transient var`
 *     buffers), and
 *   - it excludes the many immutable `val`s that the Scala compiler emits as non-`final` fields
 *     (trait members, lazy-val backing fields), which are not per-evaluation mutable state and
 *     would otherwise be false positives.
 *
 * The check reflects over every concrete [[Expression]] subclass it can reach: those registered in
 * [[FunctionRegistry]], plus those discovered by a classpath scan under `org.apache.spark`. The
 * classpath scan is what reaches API-only expressions such as [[UserDefinedGenerator]], which is
 * built through the DataFrame API and never registered as a named SQL function. Lazy-val backing
 * fields are excluded structurally; verified exceptions are documented in [[knownSafe]].
 */
class StatefulExpressionAuditSuite extends SparkFunSuite {

  /**
   * Built-in expressions that declare a mutable `@transient` field but are verified NOT to hold
   * per-evaluation mutable state, so they need not be `stateful`. Every entry must document why it
   * is safe. Entries are validated against the live scan by
   * `known-safe allowlist has no stale entries` below, so a class that stops being flagged (e.g.
   * because it was made `stateful` or its field removed) must be deleted from here.
   */
  private val knownSafe: Map[String, String] = Map(
    classOf[SparkPartitionID].getName ->
      ("partitionId is a per-partition constant: initializeInternal() sets it once per task and " +
        "every row of the partition then reads the same value, so a shared instance cannot be " +
        "corrupted across concurrent evaluation.")
  )

  /**
   * Returns the names of `@transient` instance fields on `cls` (walking up to, but excluding,
   * [[Expression]]) that look like mutable per-evaluation state: non-static, non-`final`, and not
   * a lazy-val backing field. Lazy vals are recognised by their generated `<name>$lzycompute`
   * accessor; the `bitmap$` guard fields and the `Nondeterministic.initialized` flag are excluded
   * as framework plumbing.
   */
  private def mutableStateFieldNames(cls: Class[_]): Seq[String] = {
    // Collect every declared method name across the whole hierarchy (superclasses and the traits
    // that become interfaces) so we can spot the `<name>$lzycompute` accessor that Scala emits for
    // a lazy val -- a lazy val's backing field is also non-final and would otherwise look mutable.
    val methodNames = scala.collection.mutable.Set.empty[String]
    val visited = scala.collection.mutable.Set.empty[Class[_]]
    def collectMethods(c: Class[_]): Unit = {
      if (c != null && visited.add(c)) {
        c.getDeclaredMethods.foreach(m => methodNames += m.getName)
        collectMethods(c.getSuperclass)
        c.getInterfaces.foreach(collectMethods)
      }
    }
    collectMethods(cls)

    val fields = scala.collection.mutable.LinkedHashSet.empty[String]
    var c: Class[_] = cls
    while (c != null && classOf[Expression].isAssignableFrom(c) && c != classOf[Expression]) {
      c.getDeclaredFields.foreach { f =>
        val m = f.getModifiers
        val name = f.getName
        val syntheticLazy = name.contains("bitmap$") || name.contains("$lzy") ||
          name.endsWith("$initialized") || name.endsWith("$module") || name.contains("MODULE$")
        val isLazyValBacking = methodNames.contains(name + "$lzycompute")
        if (Modifier.isTransient(m) && !Modifier.isStatic(m) && !Modifier.isFinal(m) &&
            !f.isSynthetic && !syntheticLazy && !isLazyValBacking) {
          fields += name
        }
      }
      c = c.getSuperclass
    }
    fields.toSeq
  }

  /**
   * True if `cls` (or any class / trait in its hierarchy below [[Expression]]) overrides
   * `stateful`. Every override in the codebase returns `true`, so an override is equivalent to
   * `stateful == true` without needing to instantiate the expression.
   */
  private def overridesStateful(cls: Class[_]): Boolean = {
    val seen = scala.collection.mutable.Set.empty[Class[_]]
    def declaresStateful(c: Class[_]): Boolean =
      c.getDeclaredMethods.exists(m => m.getName == "stateful" && m.getParameterCount == 0)
    def walk(c: Class[_]): Boolean = {
      if (c == null || c == classOf[Expression] || !classOf[Expression].isAssignableFrom(c) ||
          !seen.add(c)) {
        false
      } else {
        declaresStateful(c) || walk(c.getSuperclass) || c.getInterfaces.exists(walk)
      }
    }
    walk(cls)
  }

  /** All concrete built-in expression classes registered in the function registry. */
  private def registeredExpressionClasses(): Seq[Class[_ <: Expression]] = {
    FunctionRegistry.expressions.values
      .map(_._1.getClassName)
      .toSeq
      .distinct
      .flatMap { name =>
        val loaded =
          try Some(Utils.classForName(name)) catch { case _: Throwable => None }
        loaded
          .filter(classOf[Expression].isAssignableFrom)
          .map(_.asInstanceOf[Class[_ <: Expression]])
      }
      .filterNot(c => Modifier.isAbstract(c.getModifiers))
  }

  /**
   * Every concrete [[Expression]] subclass reachable by a classpath scan under `org.apache.spark`,
   * not just those registered in [[FunctionRegistry]]. This reaches API-only expressions -- e.g.
   * [[UserDefinedGenerator]], built via the DataFrame API and never registered as a named SQL
   * function -- that the registry-based scan can never see.
   *
   * Caveats of the classpath approach:
   *   - Guava's `getTopLevelClassesRecursive` finds only top-level classes, not nested ones, so an
   *     expression declared inside another class/object is still missed.
   *   - the scan is bounded by this module's test classpath, so expressions defined in downstream
   *     modules (e.g. `sql/core`, `hive`) are not visible from here.
   *   - `info.load()` loads (without initializing) each class; classes that fail to link (missing
   *     optional deps) are skipped.
   */
  private def classpathExpressionClasses(): Seq[Class[_ <: Expression]] = {
    val loader = Thread.currentThread.getContextClassLoader
    ClassPath.from(loader)
      .getTopLevelClassesRecursive("org.apache.spark")
      .asScala
      .toSeq
      .flatMap { info =>
        try {
          val c = info.load()
          if (classOf[Expression].isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers)) {
            Some(c.asInstanceOf[Class[_ <: Expression]])
          } else {
            None
          }
        } catch {
          case _: Throwable => None
        }
      }
      .distinct
  }

  /**
   * The full set of expressions the audit reflects over: the union of the [[FunctionRegistry]]
   * entries and the classpath scan, de-duplicated by class name.
   */
  private def candidateExpressionClasses(): Seq[Class[_ <: Expression]] = {
    (registeredExpressionClasses() ++ classpathExpressionClasses())
      .groupBy(_.getName)
      .map(_._2.head)
      .toSeq
  }

  test("SPARK-58154: built-in expressions with mutable transient state must be marked stateful") {
    val classes = candidateExpressionClasses()
    assert(classes.nonEmpty, "expected to find candidate expression classes")

    val flagged = classes.map(c => c -> mutableStateFieldNames(c)).filter(_._2.nonEmpty)

    val violations = flagged.filterNot { case (c, _) =>
      overridesStateful(c) || knownSafe.contains(c.getName)
    }

    assert(
      violations.isEmpty,
      violations
        .map { case (c, fs) =>
          s"  ${c.getName} holds mutable transient field(s) [${fs.mkString(", ")}] " +
            "but is not stateful"
        }
        .mkString(
          "The following built-in expressions declare mutable transient instance state but do " +
            "not override Expression.stateful (see SPARK-58154). Mark them " +
            "`override def stateful = true` (and, for leaf expressions, override " +
            "withNewChildrenInternal to return a fresh instance), or if the field is verified " +
            "safe add the class to `knownSafe` with a justification:\n",
          "\n",
          ""))
  }

  test("SPARK-58154: known-safe allowlist has no stale entries") {
    val classes = candidateExpressionClasses()
    val flaggedNames = classes.filter(c => mutableStateFieldNames(c).nonEmpty).map(_.getName).toSet
    val stale = knownSafe.keys.filterNot(flaggedNames.contains).toSeq.sorted
    assert(
      stale.isEmpty,
      stale.mkString(
        "These `knownSafe` entries are no longer flagged as holding mutable transient state " +
          "(they may have been made stateful or had their field removed). Delete them from the " +
          "allowlist:\n  ",
        "\n  ",
        ""))
  }
}
