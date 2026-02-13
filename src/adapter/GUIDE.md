# Adapter Guide

General guidance for working on the adapter layer (`src/adapter/`), the
coordinator, pgwire frontend, and related crates. This is a living document -
add to it as you discover invariants, pitfalls, or non-obvious design
decisions.

## Architecture & Key Concepts

<!-- TODO: fill in -->

## Correctness Invariants

### Timestamp selection must respect real-time bounds

For strict serializability, the timestamp assigned to a query must fall within
the query's real-time interval --- between the moment it arrives and the moment
its response is sent. The design doc
(`doc/developer/design/20220516_transactional_consistency.md`) states this as:

> Each timestamp is assigned to an event at some real-time moment within the
> event's bounds (its start and stop).

This means:

- **Never select a timestamp from before the query arrived.** Doing so would
  allow a query to observe a snapshot that predates its own start, violating
  the real-time ordering constraint of strict serializability.

- **Selecting a later timestamp is always safe**, as long as it is still within
  the query's real-time bounds (i.e. the query hasn't returned yet). Pushing a
  timestamp forward only makes the query appear to have executed later, which
  is fine --- the query was indeed still in-flight at that moment.

#### Why the batching oracle is correct

`BatchingTimestampOracle` collects multiple `read_ts` requests that arrive
concurrently and serves them all with a single call to the backing oracle.
Because the backing oracle is called *during* all of their real-time intervals
(after all requests arrived, before any have returned), the returned timestamp
is within bounds for every request. Batching can only push timestamps later,
never earlier. See the comment on the `BatchingTimestampOracle` struct.

#### Why caching an oracle result is not correct

It is tempting to cache or snapshot the most recent `read_ts` result (e.g. in a
shared atomic) and reuse it for subsequent queries without another oracle call.
This is wrong: the cached value was determined at a real-time moment *before*
the new query arrived, so assigning it to the new query places the query's
timestamp outside its real-time bounds. A query arriving after the cached value
was last updated would receive a stale timestamp --- one that predates its own
start --- violating strict serializability.
