package com.itextos.beacon;

import org.springframework.context.annotation.Configuration;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;

@Configuration
public class ForceNettyConfig {
    
    @Bean
    public ReactiveWebServerFactory reactiveWebServerFactory() {
        return new NettyReactiveWebServerFactory();
    }
}
