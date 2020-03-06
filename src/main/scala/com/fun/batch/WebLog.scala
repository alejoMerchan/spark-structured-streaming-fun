package com.fun.batch

import java.sql.Timestamp

case class WebLog(host: String, timestamp: Timestamp, request: String, http_reply: Int, bytes: Long)
