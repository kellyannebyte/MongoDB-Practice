// jobs queries

// 1.
// Find all of the possible values for the Career Level field. Only show each value once.
db.jobs.distinct("Career Level")

// 2.
// Show the Agency, Business Title, and Salary Range To of the top 3 hourly jobs 
// based on salary, using the "high" end of the hourly rate. Only show results for 
// jobs posted (Posting Type) externally. 
db.jobs.find({
    "Posting Type": "External",
    "Salary Frequency": "Hourly"
  }, {
    "Agency": 1,
    "Business Title": 1,
    "Salary Range To": 1,
    "_id": 0
  }).sort({ "Salary Range To": -1 }).limit(3)

// 3.
// Show the number of external (Posting Type) and full time (Full-Time/Part-Time indicator) 
// job postings per agency, along with the name of the agency. Sort in descending order of count.
db.jobs.aggregate([
    {
        $group: {
            _id: {
                "Posting Type": "$Posting Type",
                "Full-Time/Part-Time indicator": "$Full-Time/Part-Time indicator",
                "Agency": "$Agency"
            },
            count: { $sum: 1 }
        }
    },
    {
        $match: {
            "_id.Posting Type": "External",
            "_id.Full-Time/Part-Time indicator": "F"
        }
    },
    {
        $project: {
            "_id": 0,
            "Agency": "$_id.Agency",
            "count": 1
        }
    },
    {
        $sort: {
            "count": -1
        }
    }
])


// 4.
// Modify the previous query so that only agencies with more than 100 external 
// full time job postings are shown.
db.jobs.aggregate([
    {
        $group: {
            _id: {
                "Posting Type": "$Posting Type",
                "Full-Time/Part-Time indicator": "$Full-Time/Part-Time indicator",
                "Agency": "$Agency"
            },
            count: { $sum: 1 }
        }
    },
    {
        $match: {
            "_id.Posting Type": "External",
            "_id.Full-Time/Part-Time indicator": "F",
            "count": { $gt: 100 }
        }
    },
    {
        $project: {
            "_id": 0,
            "Agency": "$_id.Agency",
            "count": 1
        }
    },
    {
        $sort: {
            "count": -1
        }
    }
])


// 5.
// Modify the previous query so that it outputs: 
// the name of the agency field is agency, does not include the _id field, 
// and the agency is lowercase (use $toLower)
db.jobs.aggregate([
    {
        $group: {
            _id: {
                "Posting Type": "$Posting Type",
                "Full-Time/Part-Time indicator": "$Full-Time/Part-Time indicator",
                "Agency": "$Agency"
            },
            count: { $sum: 1 }
        }
    },
    {
        $match: {
            "_id.Posting Type": "External",
            "_id.Full-Time/Part-Time indicator": "F",
            "count": { $gt: 100 }
        }
    },
    {
        $project: {
            "_id": 0,
            "agency": { $toLower: "$_id.Agency" },
            "count": 1
        }
    },
    {
        $sort: {
            "count": -1
        }
    }
])


// 6.
// Find the average high end salary (Salary Range To) per year based on the Posting Date
// to do this, use $split and $arrayElemAt for extract the date
// use $group and $avg to find the average per year
db.jobs.aggregate([
    {
        $project: {
            "year": { $arrayElemAt: [{ $split: ["$Posting Date", "/"] }, 2] },
            "Salary Range To": 1
        }
    },
    {
        $group: {
            _id: "$year",
            "avgSalary": { $avg: "$Salary Range To" }
        }
    }
])
