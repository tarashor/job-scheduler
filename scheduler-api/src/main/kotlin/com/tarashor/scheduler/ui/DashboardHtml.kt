package com.tarashor.scheduler.ui

object DashboardHtml {
    private val content: String by lazy {
        DashboardHtml::class.java.getResourceAsStream("/static/index.html")
            ?.bufferedReader()
            ?.readText()
            ?: "<html><body><h1>Dashboard template not found in resources</h1></body></html>"
    }

    fun render(): String = content
}
