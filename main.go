package main

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/mammenj/mysqtomongo/mongodb"

	"time"
)

type mongoConfig struct {
	Host   string
	Db     string
	DbColl string
}

func main() {
	start := time.Now()
	//log stuff
	logFile, logerr := os.OpenFile("mysqltomongo.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if logerr != nil {
		panic(logerr)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	//mongodb stuff
	mgconfig, cfgErr := getMongoConfiguration()
	if cfgErr != nil {
		log.Fatalf("Error in reading mongodb config file :: %v", cfgErr)
	}
	mgoCollection := mgconfig.DbColl
	mgoDB := mgconfig.Db
	mgoHost := mgconfig.Host
	if mgoHost == "" {
		log.Fatalln("Invalid DB host ", mgoHost)
	}
	log.Printf("::::: mgoconf is %s, %s, %s", mgoHost, mgoDB, mgoCollection)

	readstart := time.Now()
	dao := UserFactoryDao("mysql")
	users, dberr := dao.GetUsers(200000)
	if dberr != nil {
		log.Fatalf("Errror from db %v", dberr)
	}
	numofUsers := len(users)
	readelapsed := time.Since(readstart)
	log.Printf("::::: Time taken to read from db for %d users %s\n", numofUsers, readelapsed)
	writestart := time.Now()
	docs := make([]interface{}, numofUsers)

	for i := 0; i < numofUsers; i++ {
		docs[i] = users[i]
	}

	mgoClient, err := mongodb.NewMongoClient(mgoHost, mgoDB, mgoCollection)
	if err != nil {
		log.Fatalf("Error getting connection to Mongodb %v", err)
	}

	cerr := mgoClient.CreateUsers(docs)
	if cerr != nil {
		log.Printf("Error creating Users %v", cerr)
	}

	writelapsed := time.Since(writestart)
	log.Printf("Inserted %d users ", numofUsers)
	log.Printf("::::: Time taken to write to MongoDB for %d users %s\n", numofUsers, writelapsed)

	//}

	elapsed := time.Since(start)
	log.Printf("::::: Total time taken to read from MySQL and write %d users %s\n", numofUsers, elapsed)

	mgoreadStart := time.Now()

	results, err := mgoClient.GetUserFromDB()

	log.Println("Got users  ", len(results))

	if err != nil {
		log.Fatalln("Error reading from Mongodb>> ", err)
	}
	mgoreadElapsed := time.Since(mgoreadStart)
	log.Printf("::::: Total time taken to read from MongoDB for %d users %s\n", numofUsers, mgoreadElapsed)

}

// getMongoConfiguration from json
func getMongoConfiguration() (mongoConfig, error) {
	myconfig := mongoConfig{}

	file, err := os.Open("." + string(os.PathSeparator) + "mgoconfig.json")
	if err != nil {
		return myconfig, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&myconfig)
	if err != nil {
		return myconfig, err
	}
	return myconfig, nil
}
