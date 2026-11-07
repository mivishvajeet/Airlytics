// Source declarations for upstream raw BigQuery tables
declare({
  schema: "raw_flights",
  name: "raw_pings",
  description: "Raw streaming telemetry pings landed by Apache Beam / Dataflow"
});

declare({
  schema: "raw_flights",
  name: "late_pings",
  description: "Late flight pings arriving beyond streaming watermark allowed lateness"
});

declare({
  schema: "raw_flights",
  name: "raw_airports",
  description: "Staging table loaded from OurAirports CSV dataset in GCS"
});
