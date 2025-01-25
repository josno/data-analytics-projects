/* Business Problem Queries */

/* 1. What are the most popular trackers for women with a medical condition status of “Watch”? */

db.medicalRecords.aggregate([
	{
		$match: {
			gender: "F",
			medicalConditions: "Watch",
		},
	},
	{
		$group: {
			_id: "$tracker",
			count: {
				$sum: 1,
			},
		},
	},
	{
		$sort: {
			count: -1,
		},
	},
	{
		$limit: 10,
	},
]);

// 2. Update a specific medicalRecords document by looking up a patient ID 965 and inserting additional values to the allergies field.
db.medicalRecords.aggregate([
	{
		$match: {
			patientId: 965,
		},
	},
	{
		$set: {
			allergies: {
				$concatArrays: [
					{
						$split: ["$allergies", ", "],
					},
					["milk", "peanuts"],
				],
			},
		},
	},
]);

// 3. What are the most highly rated brands (above average rating of 4) based on device type?
db.fitnessTrackers.aggregate([
	{
		$match: {
			ratingOutOfFive: { $gt: 4 },
		},
	},
	{
		$group: {
			_id: {
				deviceType: "$deviceType",
				brandName: "$brandName",
			},
			count: { $sum: 1 },
		},
	},
	{
		$group: {
			_id: "$_id.deviceType",
			brands: {
				$push: {
					brandName: "$_id.brandName",
					count: "$count",
				},
			},
		},
	},
	{
		$project: {
			brands: {
				$arrayToObject: {
					$map: {
						input: {
							$sortArray: {
								input: "$brands",
								sortBy: { count: -1 },
							},
						},
						as: "brand",
						in: {
							k: "$$brand.brandName",
							v: "$$brand.count",
						},
					},
				},
			},
		},
	},
]);
