package com.programmingtechie.order_service.controller;

import com.programmingtechie.order_service.dto.OrderResquest;
import com.programmingtechie.order_service.service.OrderService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderControlller {

    private final OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
//    @CircuitBreaker(name = "inventory", fallbackMethod = "fallbackMethod")
    public String placeOrder(@RequestBody OrderResquest orderResquest){
        return orderService.placeOrder(orderResquest);
    }

//    @ResponseStatus(HttpStatus.BAD_REQUEST)
//    public String fallbackMethod(OrderResquest orderResquest, RuntimeException runtimeException){
//        return "Oops! Something went wrong, please order after some time!";
//    }
}
