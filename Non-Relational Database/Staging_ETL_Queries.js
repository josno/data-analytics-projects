/* Command line scripts and MongoDB Shell Scripts*/

/* Local Terminal mongoimport Commands: 
  - Executed inside directory with relevant JSON docs
  - mongoimport needs to be saved in same path as MongoDB server

  // Create staging database and staging medicalRecords collection
mongoimport -d D597Task2Staging -c medicalRecords --jsonArray --file medical.json

  // create staging fitnessTrackers collect
mongoimport -d D597Task2Staging -c fitnessTrackers --jsonArray --file Task2Scenario1\ Dataset\ 1_fitness_trackers.json

Initiate Mongo Shell:

mongosh
*/

/* Mongo Shell Queries - Database and Data Cleaning */

// Switch to staging database
use D597Task2Staging;

// Show created collections
show collections;

// Count all documents in each collection
db.medicalRecords.countDocuments();
db.fitnessTrackers.countDocuments();

// Check type of document that was imported
db.medicalRecords.find({}).limit(1);
db.fitnessTrackers.find({}).limit(1);


/* medicalRecords Collection Queries */


// Return all unique fields 
db.medicalRecords.aggregate([
    { $project: { keys: { $objectToArray: "$$ROOT" } } },
    { $group: { _id: "$keys.k" } }
]).forEach( function (doc) { print(doc._id) });


// Get average field count per document for medicalRecords
db.medicalRecords.aggregate([
    { $project: {
        key_count: {
            $cond: { 
                if: { 
                 $isArray: { $objectToArray: "$$ROOT"}},
                then: {
                    $size: {
                        $objectToArray: "$$ROOT"
                    }
                }, 
                else: 0 }
        }
    } 
},
    { $group: {
        _id: null,
        avg_keys_per_document: {
            $avg: "$key_count"
            }
        }
    }
]);


// Check nulls for medicalRecords
db.medicalRecords.find({
    "$or": [
        { "allergies": null },
        { "Tracker": null },
        { "gender": null },
        { "patient_id": null },
        { "date_of_birth": null },
        { "medications": null },
        { "name": null },
        { "last_appointment_date": null },
        { "medical_conditions": null }
    ]
});


//Check Duplicates for medicalRecords
db.medicalRecords.aggregate([
    { 
        "$group": { 
        "_id": 
            { 
              "name": "$name", 
              "date_of_birth": "$date_of_birth", 
              "gender": "$gender" 
            }, 
        "count": 
            { "$sum": 1 } 
        } 
    }, 
    { 
        "$match": { 
        "_id": { "$ne": null }, 
        "count": { "$gt": 1 } 
        } 
    }, 
    { 
        "$project": { 
            "name": "$_id", "_id": 0 
        } 
    }
]);


// Check unique values
db.medicalRecords.distinct("name");
  // check all field names

/*  
  "allergies"
  "Tracker"
  "gender"
  "patient_id"
  "date_of_birth"
  "medications"
  "name"
  "last_appointment_date"
  "medical_conditions"
*/


// Reformat field names
db.medicalRecords.updateMany({}, 
    {
        $rename: {
            "patient_id": "patientId",
            "date_of_birth":"dateOfBirth", 
            "medical_conditions":"medicalConditions",
            "last_appointment_date": "lastAppointmentDate",
            "Tracker": "tracker"
        }
    }, 
    false,
    true
);


// Update field data types
db.medicalRecords.updateMany({},
    [
        { "$set": { "dateOfBirth": {"$toDate": "$dateOfBirth"}}},
        { "$set": { "lastAppointmentDate": {"$toDate": "$lastAppointmentDate"}}},
        { "$set": {
            "medications": {
              $cond: {
                if: {
                  $eq: ["$medications", "Yes"]
                },
                then: true,
                else: false
              }
            }
          }
        },
    ]
);


// Generate object of unique field names and updated data types
db.medicalRecords.aggregate([
    {
      "$project": {
        "_id": { "$type": "$_id" },
        "patientId": { "$type": "$patientId" },
        "name": { "$type": "$name" },
        "dateOfBirth": { "$type": "$dateOfBirth" },
        "gender": { "$type": "$gender" },
        "medicalConditions": { "$type": "$medicalConditions" },
        "medications": { "$type": "$medications" },
        "allergies": { "$type": "$allergies" },
        "lastAppointmentDate": { "$type": "$lastAppointmentDate" },
        "tracker": { "$type": "$tracker" }
      }
    },
    {
      "$group": {
        "_id": null,
        "types": {
          "$mergeObjects": "$$ROOT"
        }
      }
    },
    {
      "$replaceRoot": {
        "newRoot": "$types"
      }
    }
]);


/* fitnessTrackers Collection Queries */


// Return all unique fields 
db.fitnessTrackers.aggregate([
    { $project: { keys: { $objectToArray: "$$ROOT" } } },
    { $group: { _id: "$keys.k" } }
]).forEach( function (doc) { print(doc._id) });


// Get average field count per document for fitnessTrackers
db.fitnessTrackers.aggregate([
    { $project: {
        key_count: {
            $cond: { 
                if: { 
                 $isArray: { $objectToArray: "$$ROOT"}},
                then: {
                    $size: {
                        $objectToArray: "$$ROOT"
                    }
                }, 
                else: 0 }
        }
    } 
},
    { $group: {
        _id: null,
        avg_keys_per_document: {
            $avg: "$key_count"
            }
        }
    }
]);



// Check nulls 
db.fitnessTrackers.find({
    "$or": [
        { "Brand Name": null },
        { "Device Type": null },
        { "Model Name": null },
        { "Color": null },
        { "Selling Price": null },
        { "Original Price": null },
        { "Display": null },
        { "Rating (Out of 5)": null },
        { "Strap Material": null },
        { "Average Battery Life (in days)": null },
        { "Reviews": null}
    ]
});


// Check duplicates 
db.fitnessTrackers.aggregate([
    { 
        "$group": { 
            "_id": 
            {   
                "Brand Name": "$Brand Name", 
                "Device Type": "$Device Type",  
                "Model Name": "$Model Name", 
                "Color": "$Color", 
                "Selling Price": "$Selling Price", 
                "Original Price": "$Original Price", 
                "Display": "$Display", 
                "Rating (Out of 5)": "$Rating (Out of 5)",
                "Strap Material": "$Strap Material", 
                "Average Battery Life (in days)": "$Average Battery Life (in days)", 
                "Reviews": "$Reviews",
            }, 
            "count": 
            { "$sum": 1 } 
        } 
    }, 
    { 
        "$match": { 
            "_id": { "$ne": null }, 
            "count": { "$gt": 1 } 
        } 
    }, 
    { 
        "$project": { 
            "name": "$_id", "_id": 0 
        } 
    }
]);

/* Four objects will output indicating duplicated records that should be fixed 
   
  The find() method below generates results of the duplicated records. 
  The method is repeated four times with each unique set of target group keys to retrieve the duplicated records. 
    
  Each result from the find() queries will return two documents - one of the documents must be deleted.
  The deleteOne() methods below removes the target documents by objectID.
    
*/

db.fitnessTrackers.find({
    'Brand Name': 'FitBit',
    'Device Type': 'Smartwatch',
    'Model Name': 'Versa 3',
    Color: 'Black, Blue, Pink',
    'Selling Price': '17,999',
    'Original Price': '18,999',
    Display: 'AMOLED Display',
    'Rating (Out of 5)': 4.3,
    'Strap Material': 'Elastomer',
    'Average Battery Life (in days)': 7,
    Reviews: ''
});

db.fitnessTrackers.find({
  'Brand Name': 'APPLE',
    'Device Type': 'Smartwatch',
    'Model Name': 'Series 6 GPS 44 mm Blue Aluminium Case',
    Color: 'Deep Navy',
    'Selling Price': '43,900',
    'Original Price': '43,900',
    Display: 'OLED Retina Display',
    'Rating (Out of 5)': 4.5,
    'Strap Material': 'Aluminium',
    'Average Battery Life (in days)': 1,
    Reviews: ''
});

db.fitnessTrackers.find({
  'Brand Name': 'APPLE',
    'Device Type': 'Smartwatch',
    'Model Name': 'Series 6 GPS + Cellular 40 mm Red Aluminium Case',
    Color: 'Red',
    'Selling Price': '45,690',
    'Original Price': '49,900',
    Display: 'OLED Retina Display',
    'Rating (Out of 5)': 4.5,
    'Strap Material': 'Aluminium',
    'Average Battery Life (in days)': 1,
    Reviews: ''
});

db.fitnessTrackers.find({
  'Brand Name': 'GARMIN',
  'Device Type': 'Smartwatch',
  'Model Name': 'Vivomove 3S',
  Color: 'Black',
  'Selling Price': '19,990',
  'Original Price': '25,990',
  Display: 'AMOLED Display',
  'Rating (Out of 5)': '',
  'Strap Material': 'Silicone',
  'Average Battery Life (in days)': 14,
  Reviews: ''
});


// Delete the duplicated record IDs (example with selected target duplicated documents)
  // Iterated to delete duplicated record of each result above
  // Copy and paste to reproduce the same deletions
db.fitnessTrackers.deleteOne({_id: ObjectId('6780672313cac2cf20345ff3')});
db.fitnessTrackers.deleteOne({_id: ObjectId('6780672313cac2cf20346148')});
db.fitnessTrackers.deleteOne({_id: ObjectId('6780672313cac2cf20346140')});
db.fitnessTrackers.deleteOne({_id: ObjectId('6780672313cac2cf203461c0')});


// Check unique values to determine data types to use
db.fitnessTrackers.distinct("Brand Name");
  // check all field names
/*  
  "Brand Name"
  "Device Type"
  "Model Name"
  "Color"
  "Selling Price"
  "Original Price"
  "Display"
  "Rating (Out of 5)"
  "Strap Material"
  "Average Battery Life (in days)"=
  "Reviews"
*/ 

// Update field names with camelCase
db.fitnessTrackers.updateMany({}, 
    {
        $rename: {
            "Brand Name":"brandName", 
            "Device Type":"deviceType",
            "Model Name": "modelName",
            "Color": "color",
            "Selling Price":"sellingPrice", 
            "Original Price":"originalPrice",
            "Display": "display",
            "Rating (Out of 5)": "ratingOutOfFive",
            "Strap Material":"strapMaterial", 
            "Average Battery Life (in days)":"avgBatteryLifeInDays",
            "Reviews": "reviews",
        }
    }, 
    false, 
    true
);


// Remove commas in price strings that will change to numeric
db.fitnessTrackers.updateMany({},
    [
        { "$set": {
            "sellingPrice": {
              $replaceAll: {
                input: "$sellingPrice",
                find: ",",
                replacement: ""
              }
            }
        }},
        { "$set": { "originalPrice": {
            $replaceAll: {
              input: "$originalPrice",
              find: ",",
              replacement: ""
            }
          }},
        },
    ]
);


// Remove commas in reviews field for string values, if value is '' then change to zero
db.fitnessTrackers.updateMany({},
    [
        {"$addFields": {
            reviews: {
              $switch: {
                branches: [
                  {
                    case: {
                      $eq: ["$reviews", " "]
                    },
                    then: 0
                  },
                  {
                    case: {
                      $and: [
                        {
                          $isNumber: "$reviews"
                        },
                        {
                          $ne: ["$reviews", " "]
                        }
                      ]
                    },
                    then: "$reviews"
                  },
                  {
                    case: {
                      $isNumber: {
                        $convert: {
                          input: {
                            $replaceAll: {
                              input: "$reviews",
                              find: ",",
                              replacement: ""
                            }
                          },
                          to: "int",
                          onError: null,
                          onNull: null
                        }
                      }
                    },
                    then: {
                      $convert: {
                        input: {
                          $replaceAll: {
                            input: "$reviews",
                            find: ",",
                            replacement: ""
                          }
                        },
                        to: "int",
                        onError: 0,
                        onNull: 0
                      }
                    }
                  }
                ],
                default: 0
              }
            }
          }
        }
    ]
);


// Change colors to array field, some fields are comma separate string; convert everything to array with string elements
db.fitnessTrackers.updateMany({},
    [{ "$set": 
        {
            "color": {
              $cond: {
                if: {
                  $isArray: "$color"
                },
                then: "$color",
                else: {
                  $split: ["$color", ", "]
                }
              }
            }
          }
    }]
);


// Change numeric data types in fitnessTrackers
db.fitnessTrackers.updateMany({},
    [
        { "$set": { "sellingPrice": {"$toDouble": "$sellingPrice"}}},
        { "$set": { "originalPrice": {"$toDouble": "$originalPrice"}}},
        { "$set": { "avgBatteryLifeInDays": {"$toInt": "$avgBatteryLifeInDays"}}},
        { "$set": {
            "ratingOutOfFive": {
              $cond: {
                if: {
                  $eq: ["$ratingOutOfFive", '']
                },
                then: 0.0,
                else: { "$toDouble": "$ratingOutOfFive" }
              }
            }
          }
        },
    ]
);


// Get Unique Fields and Confirm Data Types
db.fitnessTrackers.aggregate([
    {
      "$project": {
        "_id": { "$type": "$_id" },
        "brandName": { "$type": "$brandName" },
        "deviceType": { "$type": "$deviceType" },
        "modelName": { "$type": "$modelName" },
        "color": { "$type": "$color" },
        "sellingPrice": { "$type": "$sellingPrice" },
        "originalPrice": { "$type": "$originalPrice" },
        "strapMaterial": { "$type": "$strapMaterial" },
        "display": { "$type": "$display" },
        "ratingOutOfFive": { "$type": "$ratingOutOfFive" },
        "avgBatteryLifeInDays": { "$type": "$avgBatteryLifeInDays" },
        "reviews": { "$type": "$reviews" }
      }
    },
    {
      "$group": {
        "_id": null,
        "types": {
          "$mergeObjects": "$$ROOT"
        }
      }
    },
    {
      "$replaceRoot": {
        "newRoot": "$types"
      }
    }
]);

/* Load Cleaned Data to Permanent Staging Database 
  - The paper shows creating the permanent database and collections in Compass interface
  - To reproduce in mongo shell:

    use D597Task2; 
    db.createCollection('medicalRecords');
    db.createCollection('fitnessTrackers');

    (switch back to D597Task2Staging to copy data using commands below)
    use D597Task2Staging;

  - The code below shows the mongo shell commands to copy the staging data to the new database and collections
*/


// Copy over collection documents to permanent staging database
db.medicalRecords.aggregate([
  {
    $merge: {
      into: {
        db: "D597Task2",
        coll: "medicalRecords"
      }
    }
  }
]);


// Copy over collection documents to permanent staging database
db.fitnessTrackers.aggregate([
        {
          $merge: {
            into: {
              db: "D597Task2",
              coll: "fitnessTrackers"
            }
          }
        }
]);
 

// Drop staging database and switch to permanent database
use D597Task2Staging;
db.dropDatabase();

// Switch back to permanent database
use D597Task2;