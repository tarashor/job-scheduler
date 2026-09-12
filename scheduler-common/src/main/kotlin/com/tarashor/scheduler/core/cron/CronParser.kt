package com.tarashor.scheduler.core.cron

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

class CronParser(private val expression: String, private val zoneId: ZoneId = ZoneId.systemDefault()) {

    private val parsedCron: ParsedCronSpec = parse(expression)

    fun nextExecution(afterEpochMs: Long): Long {
        val start = Instant.ofEpochMilli(afterEpochMs).atZone(zoneId).truncatedTo(ChronoUnit.MINUTES).plusMinutes(1)
        var candidate = start

        // Search up to 5 years into the future to prevent infinite loop
        val limit = start.plusYears(5)

        while (candidate.isBefore(limit)) {
            if (!parsedCron.matchesMonth(candidate.monthValue)) {
                candidate = candidate.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0)
                continue
            }

            if (!parsedCron.matchesDayOfMonth(candidate.dayOfMonth) || !parsedCron.matchesDayOfWeek(candidate.dayOfWeek.value)) {
                candidate = candidate.plusDays(1).withHour(0).withMinute(0)
                continue
            }

            if (!parsedCron.matchesHour(candidate.hour)) {
                candidate = candidate.plusHours(1).withMinute(0)
                continue
            }

            if (!parsedCron.matchesMinute(candidate.minute)) {
                candidate = candidate.plusMinutes(1)
                continue
            }

            return candidate.toInstant().toEpochMilli()
        }

        throw IllegalStateException("Could not find next execution time within 5 years for cron: $expression")
    }

    companion object {
        fun parse(expr: String): ParsedCronSpec {
            val trimmed = expr.trim()
            val normalized = when (trimmed.lowercase()) {
                "@hourly" -> "0 * * * *"
                "@daily", "@midnight" -> "0 0 * * *"
                "@weekly" -> "0 0 * * 0"
                "@monthly" -> "0 0 1 * *"
                "@yearly", "@annually" -> "0 0 1 1 *"
                else -> trimmed
            }

            val parts = normalized.split("\\s+".toRegex())
            require(parts.size == 5) {
                "Invalid cron expression: '$expr'. Expected 5 fields: <minute> <hour> <day-of-month> <month> <day-of-week>"
            }

            return ParsedCronSpec(
                minutes = parseField(parts[0], 0, 59),
                hours = parseField(parts[1], 0, 23),
                daysOfMonth = parseField(parts[2], 1, 31),
                months = parseField(parts[3], 1, 12),
                daysOfWeek = parseDayOfWeekField(parts[4])
            )
        }

        private fun parseField(field: String, min: Int, max: Int): Set<Int> {
            if (field == "*") return (min..max).toSet()

            val result = mutableSetOf<Int>()
            val parts = field.split(",")
            for (part in parts) {
                when {
                    part.contains("/") -> {
                        val subParts = part.split("/")
                        val step = subParts[1].toInt()
                        val range = if (subParts[0] == "*") {
                            min..max
                        } else if (subParts[0].contains("-")) {
                            val r = subParts[0].split("-")
                            r[0].toInt()..r[1].toInt()
                        } else {
                            subParts[0].toInt()..max
                        }
                        for (i in range step step) {
                            if (i in min..max) result.add(i)
                        }
                    }
                    part.contains("-") -> {
                        val r = part.split("-")
                        val start = r[0].toInt()
                        val end = r[1].toInt()
                        for (i in start..end) {
                            if (i in min..max) result.add(i)
                        }
                    }
                    else -> {
                        val v = part.toInt()
                        if (v in min..max) result.add(v)
                    }
                }
            }
            return result
        }

        private fun parseDayOfWeekField(field: String): Set<Int> {
            val replaced = field.uppercase()
                .replace("SUN", "7")
                .replace("MON", "1")
                .replace("TUE", "2")
                .replace("WED", "3")
                .replace("THU", "4")
                .replace("FRI", "5")
                .replace("SAT", "6")

            // In Java DayOfWeek: 1 = Monday ... 7 = Sunday
            // If cron specifies 0 for Sunday, map it to 7
            val parsed = parseField(replaced, 0, 7)
            return parsed.map { if (it == 0) 7 else it }.toSet()
        }
    }
}

data class ParsedCronSpec(
    val minutes: Set<Int>,
    val hours: Set<Int>,
    val daysOfMonth: Set<Int>,
    val months: Set<Int>,
    val daysOfWeek: Set<Int>
) {
    fun matchesMinute(minute: Int): Boolean = minutes.contains(minute)
    fun matchesHour(hour: Int): Boolean = hours.contains(hour)
    fun matchesDayOfMonth(dom: Int): Boolean = daysOfMonth.contains(dom)
    fun matchesMonth(month: Int): Boolean = months.contains(month)
    fun matchesDayOfWeek(dow: Int): Boolean = daysOfWeek.contains(dow)
}
