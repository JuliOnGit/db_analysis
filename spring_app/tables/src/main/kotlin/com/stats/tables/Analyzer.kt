package com.stats.tables

import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component

@Component
class Analyzer : CommandLineRunner {
    override fun run(vararg args: String?) {
        print("Hello there")
    }
}