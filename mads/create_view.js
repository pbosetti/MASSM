function create_measurements_view(database_name, collection_name, view_name) {
  if (!database_name || !collection_name || !view_name) {
    throw new Error(
      "Usage: create_measurements_view(database_name, collection_name, view_name)"
    );
  }

  const database = db.getSiblingDB(database_name);
  const existing = database.getCollectionInfos({ name: view_name });

  if (existing.length > 0) {
    database.getCollection(view_name).drop();
  }

  const pipeline = [
  {
    $addFields: {
      "message.measurements.timestamp": {
        $cond: [
          {
            $eq: [
              {
                $type:
                  "$message.measurements.timestamp"
              },
              "int"
            ]
          },
          {
            $toDouble:
              "$message.measurements.timestamp"
          },
          "$message.measurements.timestamp"
        ]
      }
    }
  },
  {
    $addFields: {
      "message.measurements.timestamp": {
        $toDate: "$message.measurements.timestamp"
      }
    }
  },
  {
    $unset: [
      "message.timecode",
      "message.measurements.Time",
      "message.measurements.timestamp",
      "message.measurements.date",
      "message.topic"
    ]
  },
  {
    $out:
      /**
       * Provide the name of the output database and collection.
       */
      {
        db: "MASSM",
        coll: "accelerations"
        /*
    timeseries: {
      timeField: 'field',
      bucketMaxSpanSeconds: 'number',
      granularity: 'granularity'
    }
    */
      }
  }
];

  database.createView(view_name, collection_name, pipeline);
}
