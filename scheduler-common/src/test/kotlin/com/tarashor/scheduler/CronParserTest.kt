package com.tarashor.scheduler

import com.tarashor.scheduler.core.cron.CronParser
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CronParserTest {

    @Test
    fun `test every 5 minutes next execution`() {
        val parser = CronParser("*/5 * * * *", ZoneId.of("UTC"))
        // Base time: 2026-09-12 10:02:15 UTC
        val baseTime = ZonedDateTime.of(2026, 9, 12, 10, 2, 15, 0, ZoneId.of("UTC")).toInstant().toEpochMilli()

        val next = parser.nextExecution(baseTime)
        val nextZdt = Instant.ofEpochMilli(next).atZone(ZoneId.of("UTC"))

        assertEquals(2026, nextZdt.year)
        assertEquals(9, nextZdt.monthValue)
        assertEquals(12, nextZdt.dayOfMonth)
        assertEquals(10, nextZdt.hour)
        assertEquals(5, nextZdt.minute) // Rounded up to next 5th minute
    }

    @Test
    fun `test daily midnight preset`() {
        val parser = CronParser("@daily", ZoneId.of("UTC"))
        // Base time: 2026-09-12 14:30:00 UTC
        val baseTime = ZonedDateTime.of(2026, 9, 12, 14, 30, 0, 0, ZoneId.of("UTC")).toInstant().toEpochMilli()

        val next = parser.nextExecution(baseTime)
        val nextZdt = Instant.ofEpochMilli(next).atZone(ZoneId.of("UTC"))

        assertEquals(13, nextZdt.dayOfMonth)
        assertEquals(0, nextZdt.hour)
        assertEquals(0, nextZdt.minute)
    }

    @Test
    fun `test specific hour and weekday cron`() {
        // Run at 09:00 on Mondays
        val parser = CronParser("0 9 * * MON", ZoneId.of("UTC"))
        // Base time: Saturday 2026-09-12 10:00:00 UTC
        val baseTime = ZonedDateTime.of(2026, 9, 12, 10, 0, 0, 0, ZoneId.of("UTC")).toInstant().toEpochMilli()

        val next = parser.nextExecution(baseTime)
        val nextZdt = Instant.ofEpochMilli(next).atZone(ZoneId.of("UTC"))

        assertEquals(java.time.DayOfWeek.MONDAY, nextZdt.dayOfWeek)
        assertEquals(9, nextZdt.hour)
        assertEquals(0, nextZdt.minute)
        assertTrue(nextZdt.dayOfMonth == 14)
    }
}
