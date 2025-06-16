package sql

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

type UserResult struct {
	UIDOwner string `db:"uid_owner"`
	Count    int    `db:"count"`
}

func Migrate(username, password, host, dbName string, port int, dryRun bool) {
	// Set up db connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", username, password, host, port, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("Failed to open database: %s\n", err.Error())
	}
	defer db.Close()

	// Get list of users to migrate
	users, err := getUsersToMigrate(db)
	if err != nil {
		fmt.Printf("Failed to get users to migrate: %s\n", err.Error())
		return
	}
	fmt.Printf("Found %d users with public rw links without expiry\n", len(users))

	expiry := time.Now().AddDate(0, 3, 0) // Set expiry to 3 months from now

	// Get IDs of links for each user
	for _, r := range users {
		ids, err := getPublicRWIdsForUser(db, r)
		if err != nil {
			fmt.Printf("Failed to get public rw links for user %s: %s\n", r.UIDOwner, err.Error())
			os.Exit(1)
		}
		fmt.Printf("User %s has link with id %d\n", r.UIDOwner, ids[0])
		for _, id := range ids {
			err := setExpiryOnLink(db, id, expiry, dryRun)
			if err != nil {
				fmt.Printf("Failed to set expiry on link %d: %s\n", id, err.Error())
				os.Exit(1)
			}
		}
	}

}

func getUsersToMigrate(db *sql.DB) ([]UserResult, error) {
	var results []UserResult
	query := `
        SELECT uid_owner, COUNT(*) as count 
        FROM public_links 
        WHERE expiration IS NULL 
        AND permissions = 15 
        AND item_type = 'folder' 
        GROUP BY uid_owner 
        ORDER BY count ASC`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var result UserResult
		if err := rows.Scan(&result.UIDOwner, &result.Count); err != nil {
			return nil, err
		}
		fmt.Printf("User %s has %d public rw links\n", result.UIDOwner, result.Count)

		results = append(results, result)
	}

	return results, nil
}

func getPublicRWIdsForUser(db *sql.DB, user UserResult) ([]int, error) {
	ids := make([]int, 0, user.Count)
	query := `
        SELECT id
        FROM public_links
        WHERE uid_owner = ? AND expiration IS NULL AND permissions = 15 AND item_type = 'folder'`
	rows, err := db.Query(query, user.UIDOwner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func setExpiryOnLink(db *sql.DB, id int, expiry time.Time, dryRun bool) error {
	if dryRun {
		fmt.Printf("UPDATE public_links SET expiration = %s WHERE id = %d\n", expiry, id)
	} else {
		query := `UPDATE public_links SET expiration = ? WHERE id = ?`
		_, err := db.Exec(query, expiry, id)
		if err != nil {
			return fmt.Errorf("failed to set expiry on link %d: %w", id, err)
		}
	}

	return nil
}
