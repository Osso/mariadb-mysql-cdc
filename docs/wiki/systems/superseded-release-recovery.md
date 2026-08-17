# Superseded release recovery (retired)

The native live stream no longer contains coordinate-specific supersession or
foreign-key recovery. Historical `globalcomix.releases` transactions receive the
same source-authoritative rule as every other ROW event:

- INSERT MySQL `1062` is idempotent success.
- Foreign-key, CHECK, UPDATE, DELETE, schema, and connection errors roll back the
  complete source transaction and block checkpoint advancement.
- The stream does not read or repair target rows and does not write conflict
  evidence.

Historical conflict records remain available to offline `repair-drift` and
targeted resolution workflows. This page is retained only to make the removal of
the former special case explicit.
