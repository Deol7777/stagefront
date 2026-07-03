package com.ticketing.inventory.web;

import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only seat listing for the debug dashboard. */
@RestController
@RequestMapping("/api/seats")
public class SeatController {

    private final SeatRepository seats;

    public SeatController(SeatRepository seats) {
        this.seats = seats;
    }

    @GetMapping
    public List<SeatEntity> list() {
        return seats.findAll();
    }
}
