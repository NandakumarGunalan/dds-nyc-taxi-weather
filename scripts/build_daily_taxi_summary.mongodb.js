[
  {
    $match: {
      tpep_pickup_datetime: {
        $gte: ISODate("2022-01-01T00:00:00.000Z"),
        $lt: ISODate("2023-01-01T00:00:00.000Z")
      }
    }
  },
  {
    $addFields: {
      trip_date: {
        $dateToString: {
          format: "%Y-%m-%d",
          date: "$tpep_pickup_datetime"
        }
      },
      trip_duration_minutes: {
        $divide: [
          {
            $subtract: [
              "$tpep_dropoff_datetime",
              "$tpep_pickup_datetime"
            ]
          },
          60000
        ]
      }
    }
  },
  {
    $group: {
      _id: "$trip_date",
      total_trips: { $sum: 1 },
      avg_trip_distance: { $avg: "$trip_distance" },
      avg_fare_amount: { $avg: "$fare_amount" },
      avg_total_amount: { $avg: "$total_amount" },
      avg_passenger_count: { $avg: "$passenger_count" },
      avg_trip_duration_minutes: { $avg: "$trip_duration_minutes" }
    }
  },
  {
    $project: {
      _id: 0,
      trip_date: "$_id",
      total_trips: 1,
      avg_trip_distance: { $round: ["$avg_trip_distance", 2] },
      avg_fare_amount: { $round: ["$avg_fare_amount", 2] },
      avg_total_amount: { $round: ["$avg_total_amount", 2] },
      avg_passenger_count: { $round: ["$avg_passenger_count", 2] },
      avg_trip_duration_minutes: { $round: ["$avg_trip_duration_minutes", 2] }
    }
  },
  {
    $sort: {
      trip_date: 1
    }
  },
  {
    $out: {
      db: "dds_nyc_taxi_weather",
      coll: "daily_taxi_summary"
    }
  }
]
