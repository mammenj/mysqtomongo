package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	//mysl driver package
	_ "github.com/go-sql-driver/mysql"
	"github.com/mammenj/mysqtomongo/mongodb"
)

// UserDao abstraction
type UserDao interface {
	GetUsers(int) ([]mongodb.User, error)
}

// UserImplMysql db object
type UserImplMysql struct {
}

//SQLConfig will load all database related configurations
type sqlConfig struct {
	Engine   string
	Server   string
	Port     string
	User     string
	Password string
	Database string
}

// getDBConfiguration from json
func getDBConfiguration() (sqlConfig, error) {
	myconfig := sqlConfig{}

	file, err := os.Open("." + string(os.PathSeparator) + "dbconfig.json")
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

func getDB() *sql.DB {
	config, err := getDBConfiguration()
	if err != nil {
		log.Fatalln(err)
		return nil
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?tls=false&autocommit=true", config.User, config.Password, config.Server, config.Port, config.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalln(err)
		return nil
	}
	return db
}

// UserFactoryDao get the user factory
func UserFactoryDao(e string) UserDao {
	var dao UserDao
	switch e {
	case "mysql":
		dao = UserImplMysql{}

	default:
		log.Fatalf("Errorr %s", e)
		return nil
	}
	return dao
}

// GetUsers by limit of records
func (dao UserImplMysql) GetUsers(limit int) ([]mongodb.User, error) {

	query := "select id, name, email from user LIMIT " + strconv.Itoa(limit)

	users := make([]mongodb.User, 0)
	db := getDB()
	defer db.Close()

	stmt, err := db.Prepare(query)
	if err != nil {
		return users, err
	}

	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return users, err
	}
	for rows.Next() {
		var row mongodb.User
		err := rows.Scan(&row.ID, &row.Name, &row.Email)

		if err != nil {
			return nil, err
		}

		users = append(users, row)
	}
	return users, nil
}
