package com.tarashor.scheduler.api

import com.tarashor.scheduler.ui.DashboardHtml
import org.springframework.http.MediaType
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ResponseBody

@Controller
class DashboardController {

    @GetMapping(value = ["/", "/ui"], produces = [MediaType.TEXT_HTML_VALUE])
    @ResponseBody
    fun dashboard(): String = DashboardHtml.render()
}
