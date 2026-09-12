package com.tarashor.scheduler

import com.tarashor.scheduler.core.dag.DAGEngine
import com.tarashor.scheduler.core.dag.InvalidDAGException
import com.tarashor.scheduler.core.model.TaskAction
import com.tarashor.scheduler.core.model.TaskSpec
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DAGValidatorTest {

    @Test
    fun `test valid diamond DAG topological sort`() {
        // Diamond: A -> B, A -> C, B -> D, C -> D
        val tasks = listOf(
            TaskSpec("D", "Task D", dependencies = setOf("B", "C"), action = TaskAction.Shell("echo D")),
            TaskSpec("B", "Task B", dependencies = setOf("A"), action = TaskAction.Shell("echo B")),
            TaskSpec("C", "Task C", dependencies = setOf("A"), action = TaskAction.Shell("echo C")),
            TaskSpec("A", "Task A", dependencies = emptySet(), action = TaskAction.Shell("echo A"))
        )

        val sorted = DAGEngine.validateAndSort(tasks)

        assertEquals("A", sorted.first())
        assertEquals("D", sorted.last())
        val indexA = sorted.indexOf("A")
        val indexB = sorted.indexOf("B")
        val indexC = sorted.indexOf("C")
        val indexD = sorted.indexOf("D")

        assertTrue(indexA < indexB)
        assertTrue(indexA < indexC)
        assertTrue(indexB < indexD)
        assertTrue(indexC < indexD)
    }

    @Test
    fun `test cyclic DAG detection`() {
        // Cycle: A -> B -> C -> A
        val tasks = listOf(
            TaskSpec("A", "Task A", dependencies = setOf("C"), action = TaskAction.Shell("echo A")),
            TaskSpec("B", "Task B", dependencies = setOf("A"), action = TaskAction.Shell("echo B")),
            TaskSpec("C", "Task C", dependencies = setOf("B"), action = TaskAction.Shell("echo C"))
        )

        val ex = assertThrows<InvalidDAGException> {
            DAGEngine.validateAndSort(tasks)
        }
        assertTrue(ex.message!!.contains("Cyclic dependency detected"))
    }

    @Test
    fun `test self loop DAG detection`() {
        val tasks = listOf(
            TaskSpec("A", "Task A", dependencies = setOf("A"), action = TaskAction.Shell("echo A"))
        )

        val ex = assertThrows<InvalidDAGException> {
            DAGEngine.validateAndSort(tasks)
        }
        assertTrue(ex.message!!.contains("cannot depend on itself"))
    }

    @Test
    fun `test missing dependency detection`() {
        val tasks = listOf(
            TaskSpec("A", "Task A", dependencies = setOf("non-existent"), action = TaskAction.Shell("echo A"))
        )

        val ex = assertThrows<InvalidDAGException> {
            DAGEngine.validateAndSort(tasks)
        }
        assertTrue(ex.message!!.contains("depends on unknown task"))
    }
}
