package mongodb

import (
	mgo "gopkg.in/mgo.v2"
)

// MongoClient holds session and other related info
type MongoClient struct {
	session      *mgo.Session // master session
	uri          string       // mongodb uri
	dbName       string       // database name
	mgCollection string       // collection name
}

// NewMongoClient establishes connection to MongoDB database
func NewMongoClient(uri, dbName, mgCollection string) (*MongoClient, error) {
	session, err := mgo.Dial(uri)
	if err != nil {
		return nil, err
	}
	session.SetMode(mgo.Monotonic, true)
	return &MongoClient{session, uri, dbName, mgCollection}, nil
}

func (mc *MongoClient) getSession() *mgo.Session {
	return mc.session.Copy()
}

// CreateUsers as bulk operationu-
func (mc *MongoClient) CreateUsers(users []interface{}) error {
	s := mc.getSession()

	b := s.DB(mc.dbName).C(mc.mgCollection).Bulk()
	b.Insert(users...)
	_, err := b.Run()
	s.Close()
	return err
}

// GetUserFromDB - gets all users form DBFromDB -
func (mc *MongoClient) GetUserFromDB() ([]User, error) {
	s := mc.getSession()
	var results []User
	err := s.DB(mc.dbName).C(mc.mgCollection).Find(nil).All(&results)
	if err != nil {
		return nil, err
	}
	s.Close()
	return results, nil
}
