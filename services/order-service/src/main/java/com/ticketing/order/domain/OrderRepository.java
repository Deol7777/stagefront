package com.ticketing.order.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

/**
 * Spring Data repository for orders. Extending JpaRepository gives CRUD
 * (save, findById, findAll, ...) with no implementation to write.
 */
public interface OrderRepository extends JpaRepository<OrderEntity, String> {

    /** Look up the order a given client request already created, if any. */
    Optional<OrderEntity> findByRequestId(String requestId);

    /**
     * Take a transaction-scoped Postgres advisory lock keyed on a requestId, so
     * two concurrent place-order calls with the SAME requestId run one-at-a-time
     * through the "check then insert" section instead of racing into a unique-
     * constraint violation.
     *
     * <p>Why this and not just the unique index: the index is correct, but the
     * loser of the race learns it lost by having its INSERT throw — which
     * Hibernate logs at ERROR before Spring translates it. That's alarm-worthy
     * noise for something that is business-as-usual. The advisory lock removes the
     * collision entirely: the second caller waits, then sees the first caller's
     * committed row and returns it. No exception, no ERROR line.
     *
     * <p>Keyed on {@code hashtext(requestId)}, so DIFFERENT requestIds hash to
     * different lock keys and never contend — normal traffic stays fully parallel;
     * only genuine duplicates serialize. The lock auto-releases at transaction end.
     * It is database-wide, so this holds across multiple order-service instances,
     * not just threads in one JVM.
     */
    @Query(value = "SELECT pg_advisory_xact_lock(hashtext(:requestId))", nativeQuery = true)
    void lockForRequest(@Param("requestId") String requestId);
}
