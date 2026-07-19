package com.ticketing.order.invariant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads inventory + payment state over their public read APIs.
 *
 * <p>Why HTTP and not a database join: database-per-service is the whole point of
 * the architecture. Reaching into another service's Postgres would be the exact
 * coupling the design forbids, and a reconciler is not a good enough reason to
 * break it — it would then also break every time that service migrated a column.
 *
 * <p>The honest cost of doing it this way: these are three independent reads of
 * three independently-moving systems, so what we assemble is a SMEAR, not a
 * snapshot. See the note on false positives in {@link InvariantChecker}.
 *
 * <p>Failure handling: if a service is unreachable we log and return empty rather
 * than throwing. Reason — a reconciler that dies when one dependency is down is
 * useless precisely during an incident. Note the trade-off this creates: an empty
 * read makes confirmed orders look seat-less, so treat a sudden flood of
 * violations as "is a service down?" before "is the data corrupt?".
 */
@Component
public class HttpServiceStateReader implements InvariantChecker.ServiceStateReader {

    private static final Logger log = LoggerFactory.getLogger(HttpServiceStateReader.class);

    private final RestClient inventory;
    private final RestClient payment;

    public HttpServiceStateReader(
            RestClient.Builder builder,
            @Value("${services.inventory.base-url:http://localhost:8082}") String inventoryUrl,
            @Value("${services.payment.base-url:http://localhost:8083}") String paymentUrl) {
        this.inventory = builder.clone().baseUrl(inventoryUrl).build();
        this.payment = builder.clone().baseUrl(paymentUrl).build();
    }

    @Override
    public Map<String, InvariantChecker.SeatView> seatsByReservingOrder() {
        List<Map<String, Object>> seats = get(inventory, "/api/seats", "inventory-service");
        return seats.stream()
                .filter(seat -> seat.get("reservedByOrder") != null)
                .collect(Collectors.toMap(
                        seat -> String.valueOf(seat.get("reservedByOrder")),
                        seat -> new InvariantChecker.SeatView(
                                String.valueOf(seat.get("seatId")),
                                String.valueOf(seat.get("status")),
                                String.valueOf(seat.get("reservedByOrder"))),
                        // Two seats claiming the same order is itself broken state;
                        // keep the first and let the seat-level rules report it.
                        (a, b) -> a));
    }

    @Override
    public Map<String, List<InvariantChecker.PaymentView>> paymentsByOrder() {
        List<Map<String, Object>> payments = get(payment, "/api/payments", "payment-service");
        return payments.stream()
                .filter(p -> p.get("orderId") != null)
                .map(p -> new InvariantChecker.PaymentView(
                        String.valueOf(p.get("orderId")),
                        String.valueOf(p.get("status"))))
                .collect(Collectors.groupingBy(InvariantChecker.PaymentView::orderId));
    }

    private List<Map<String, Object>> get(RestClient client, String path, String serviceName) {
        try {
            List<Map<String, Object>> body = client.get()
                    .uri(path)
                    .retrieve()
                    .body(new ParameterizedTypeReference<>() {
                    });
            return body != null ? body : List.of();
        } catch (Exception e) {
            log.warn("Invariant check could not read {} from {} ({}); treating as empty",
                    path, serviceName, e.getMessage());
            return List.of();
        }
    }
}
