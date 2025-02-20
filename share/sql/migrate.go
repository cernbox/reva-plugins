package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	model "github.com/cernbox/reva-plugins/share"
	providerv1beta1 "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Migrator struct {
	NewDb    *gorm.DB
	OldDb    *sql.DB
	ShareMgr *shareMgr
	LinkMgr  *publicShareMgr
}

type ShareOrLink struct {
	IsShare bool
	Share   *model.Share
	Link    *model.PublicLink
}

func RunMigration(username, password, host, name, gatewaysvc, token string, port int) {
	config := map[string]interface{}{
		"engine":      "mysql",
		"db_username": username,
		"db_password": password,
		"db_host":     host,
		"db_port":     port,
		"db_name":     name,
		"gatewaysvc":  gatewaysvc,
		"dry_run":     false,
	}
	tokenlessCtx, cancel := context.WithCancel(context.Background())
	ctx := appctx.ContextSetToken(tokenlessCtx, token)
	ctx = metadata.AppendToOutgoingContext(ctx, appctx.TokenHeader, token)
	defer cancel()

	shareManager, err := NewShareManager(ctx, config)
	if err != nil {
		fmt.Println("Failed to create shareManager: " + err.Error())
		os.Exit(1)
	}
	sharemgr := shareManager.(*shareMgr)
	oldDb, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, name))
	if err != nil {
		fmt.Println("Failed to create db: " + err.Error())
		os.Exit(1)
	}
	migrator := Migrator{
		OldDb:    oldDb,
		NewDb:    sharemgr.db,
		ShareMgr: sharemgr,
	}

	ch := make(chan *ShareOrLink, 100)
	go getAllShares(ctx, migrator, ch)
	for share := range ch {
		// TODO error handling
		if share.IsShare {
			fmt.Printf("Creating share %d\n", share.Share.ID)
			migrator.NewDb.Create(&share.Share)
		} else {
			fmt.Printf("Creating share %d\n", share.Link.ID)
			migrator.NewDb.Create(&share.Link)
		}
	}

}

func getAllShares(ctx context.Context, migrator Migrator, ch chan *ShareOrLink) {
	// First we find out what the highest ID is
	count, err := getCount(migrator)
	if err != nil {
		fmt.Println("Error getting highest id: " + err.Error())
		close(ch)
		return
	}
	fmt.Printf("Migrating %d shares\n", count)

	query := "select id, coalesce(uid_owner, '') as uid_owner, coalesce(uid_initiator, '') as uid_initiator, lower(coalesce(share_with, '')) as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, coalesce(item_type, '') as item_type, stime, permissions, share_type, orphan FROM oc_share order by id desc" // AND id=?"
	params := []interface{}{}

	res, err := migrator.OldDb.Query(query, params...)

	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
		close(ch)
		return
	}

	for res.Next() {
		var s OldShareEntry
		res.Scan(&s.ID, &s.UIDOwner, &s.UIDInitiator, &s.ShareWith, &s.Prefix, &s.ItemSource, &s.ItemType, &s.STime, &s.Permissions, &s.ShareType, &s.Orphan)
		newShare, err := oldShareToNewShare(ctx, migrator, s)
		if err == nil {
			ch <- newShare
		} else {
			fmt.Printf("Error occured for share %s: %s\n", s.ID, err.Error())
		}
	}

	close(ch)
}

type OldShareEntry struct {
	ID                           int
	UIDOwner                     string
	UIDInitiator                 string
	Prefix                       string
	ItemSource                   string
	ItemType                     string
	ShareWith                    string
	Token                        string
	Expiration                   sql.NullTime
	Permissions                  int
	ShareType                    int
	ShareName                    string
	STime                        int
	FileTarget                   string
	State                        int
	Quicklink                    bool
	Description                  string
	NotifyUploads                bool
	NotifyUploadsExtraRecipients string
	Orphan                       bool
}

type OldShareState struct {
	id        int
	recipient string
	state     int
}

const (
	bufferSize = 10
	numWorkers = 5
)

func RunMigration(username, password, host, name, gatewaysvc, token string, port int, dryRun bool) {
	// Config
	config := map[string]interface{}{
		"engine":      "mysql",
		"db_username": username,
		"db_password": password,
		"db_host":     host,
		"db_port":     port,
		"db_name":     name,
		"gatewaysvc":  gatewaysvc,
		"dry_run":     dryRun,
	}

	// Authenticate to gateway service
	tokenlessCtx, cancel := context.WithCancel(context.Background())
	ctx := appctx.ContextSetToken(tokenlessCtx, token)
	ctx = metadata.AppendToOutgoingContext(ctx, appctx.TokenHeader, token)
	defer cancel()

	// Set up migrator
	shareManager, err := New(ctx, config)
	if err != nil {
		fmt.Println("Failed to create shareManager: " + err.Error())
		os.Exit(1)
	}
	sharemgr := shareManager.(*mgr)
	oldDb, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", username, password, host, port, name))
	if err != nil {
		fmt.Println("Failed to create db: " + err.Error())
		os.Exit(1)
	}
	migrator := Migrator{
		OldDb:    oldDb,
		NewDb:    sharemgr.db,
		ShareMgr: sharemgr,
	}

	if dryRun {
		migrator.NewDb = migrator.NewDb.Debug()
	}

	migrator.NewDb.AutoMigrate(&model.Share{}, &model.PublicLink{}, &model.ShareState{})

	migrateShares(ctx, migrator)
	fmt.Println("---------------------------------")
	migrateShareStatuses(ctx, migrator)

}

func migrateShares(ctx context.Context, migrator Migrator) {
	// Check how many shares are to be migrated
	count, err := getCount(migrator, "oc_share")
	if err != nil {
		fmt.Println("Error getting count: " + err.Error())
		return
	}
	fmt.Printf("Migrating %d shares\n", count)

	// Get all old shares
	query := "select id, coalesce(uid_owner, '') as uid_owner, coalesce(uid_initiator, '') as uid_initiator, lower(coalesce(share_with, '')) as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, coalesce(item_type, '') as item_type, coalesce(token, '') as token, expiration, stime, permissions, share_type, coalesce(share_name, '') as share_name, notify_uploads, coalesce(notify_uploads_extra_recipients, '') as notify_uploads_extra_recipients, orphan FROM oc_share order by id desc" // AND id=?"
	params := []interface{}{}

	res, err := migrator.OldDb.Query(query, params...)

	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
		os.Exit(1)
	}

	// Create channel for workers
	ch := make(chan *OldShareEntry, bufferSize)
	var wg sync.WaitGroup

	// Start all workers
	for range numWorkers {
		go workerShare(ctx, migrator, ch, &wg)
	}

	for res.Next() {
		var s OldShareEntry
		res.Scan(&s.ID, &s.UIDOwner, &s.UIDInitiator, &s.ShareWith, &s.Prefix, &s.ItemSource, &s.ItemType, &s.Token, &s.Expiration, &s.STime, &s.Permissions, &s.ShareType, &s.ShareName, &s.NotifyUploads, &s.NotifyUploadsExtraRecipients, &s.Orphan)
		if err == nil {
			ch <- &s
		} else {
			fmt.Printf("Error occured for share %d: %s\n", s.ID, err.Error())
		}
	}

	close(ch)
	wg.Wait()
}

func migrateShareStatuses(ctx context.Context, migrator Migrator) {
	// Check how many shares are to be migrated
	count, err := getCount(migrator, "oc_share_status")
	if err != nil {
		fmt.Println("Error getting count: " + err.Error())
		return
	}
	fmt.Printf("Migrating %d share statuses\n", count)

	// Get all old shares
	query := "select id, coalesce(recipient, '') as recipient, state FROM oc_share_status order by id desc"
	params := []interface{}{}

	res, err := migrator.OldDb.Query(query, params...)

	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
		os.Exit(1)
	}

	// Create channel for workers
	ch := make(chan *OldShareState, bufferSize)

	var wg sync.WaitGroup

	// Start all workers
	for range numWorkers {
		go workerState(ctx, migrator, ch, &wg)
	}

	for res.Next() {
		var s OldShareState
		res.Scan(&s.id, &s.recipient, &s.state)
		if err == nil {
			ch <- &s
		} else {
			fmt.Printf("Error occured for share status%d: %s\n", s.id, err.Error())
		}
	}
	close(ch)
	wg.Wait()
}

func workerShare(ctx context.Context, migrator Migrator, ch chan *OldShareEntry, wg *sync.WaitGroup) {
	wg.Add(1)
	for share := range ch {
		handleSingleShare(ctx, migrator, share)
	}
	wg.Done()
}

func workerState(ctx context.Context, migrator Migrator, ch chan *OldShareState, wg *sync.WaitGroup) {
	wg.Add(1)
	for state := range ch {
		handleSingleState(ctx, migrator, state)
	}
	wg.Done()
}

func handleSingleShare(ctx context.Context, migrator Migrator, s *OldShareEntry) {
	share, err := oldShareToNewShare(ctx, migrator, s)
	if err != nil {
		fmt.Printf("An error occured while migrating share %ds: %s\n", s.ID, err.Error())
		return
	}
	var res *gorm.DB
	if share.IsShare {
		res = migrator.NewDb.Create(&share.Share)
	} else {
		res = migrator.NewDb.Create(&share.Link)
	}

	if res.Error != nil {
		fmt.Printf("An error occured while migrating share %ds: %s\n", s.ID, res.Error.Error())
	}
}

func handleSingleState(ctx context.Context, migrator Migrator, s *OldShareState) {
	newShareState := &model.ShareState{
		ShareID: uint(s.id),
		User:    s.recipient,
		Hidden:  s.state == -1, // Hidden if REJECTED
		Synced:  false,
	}
	res := migrator.NewDb.Create(&newShareState)
	if res.Error != nil {
		fmt.Printf("An error occured while migrating share state (%d, %s): %s\n", s.id, s.recipient, res.Error.Error())
	}
}

func oldShareToNewShare(ctx context.Context, migrator Migrator, s *OldShareEntry) (*ShareOrLink, error) {
	var createdAt, updatedAt time.Time
	if s.STime != 0 {
		createdAt = time.Unix(int64(s.STime), 0)
		updatedAt = time.Unix(int64(s.STime), 0)
	} else {
		createdAt = time.Now()
		updatedAt = time.Now()
		fmt.Printf("WARN: STime not set for share %d\n", s.ID)
	}
	protoShare := model.ProtoShare{
		Model: gorm.Model{
			ID:        uint(s.ID),
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
		UIDOwner:     s.UIDOwner,
		UIDInitiator: s.UIDInitiator,
		Permissions:  uint8(s.Permissions),
		Orphan:       s.Orphan, // will be re-checked later
		Expiration: datatypes.NullTime{
			Valid: s.Expiration.Valid,
			V:     s.Expiration.Time,
		},
		ItemType:    model.ItemType(s.ItemType),
		InitialPath: "", // set later
		Inode:       s.ItemSource,
		Instance:    s.Prefix,
	}

	// Getting InitialPath
	if !protoShare.Orphan {
		path, err := migrator.ShareMgr.getPath(ctx, &providerv1beta1.ResourceId{
			StorageId: protoShare.Instance,
			OpaqueId:  protoShare.Inode,
		})
		if err == nil {
			protoShare.InitialPath = path
		} else if errors.Is(err, errtypes.NotFound(protoShare.Inode)) {
			fmt.Printf("Marked share %d as an orphan (%s, %s)\n", s.ID, protoShare.Instance, protoShare.Inode)
			protoShare.Orphan = true
		} else {
			// We do not set, because of a general error
			// fmt.Printf("An error occured for share %d while statting (%s, %s): %s\n", s.ID, protoShare.Instance, protoShare.Inode, err.Error())
		}
	}

	// ShareTypeUser = 0
	// ShareTypeGroup = 1
	// ShareTypePublicLink = 3
	// ShareTypeFederatedCloudShare = 6
	// ShareTypeSpaceMembership = 7
	if s.ShareType == 0 || s.ShareType == 1 {
		return &ShareOrLink{
			IsShare: true,
			Share: &model.Share{
				ProtoShare:        protoShare,
				ShareWith:         s.ShareWith,
				SharedWithIsGroup: s.ShareType == 1,
				Description:       s.Description,
			},
		}, nil
	} else if s.ShareType == 3 {

		return &ShareOrLink{
			IsShare: false,
			Link: &model.PublicLink{
				ProtoShare:                   protoShare,
				Token:                        s.Token,
				Quicklink:                    s.Quicklink,
				NotifyUploads:                s.NotifyUploads,
				NotifyUploadsExtraRecipients: s.NotifyUploadsExtraRecipients,
				Password:                     s.ShareWith,
				LinkName:                     s.ShareName,
			},
		}, nil
	} else {
		return nil, errors.New("Invalid share type")
	}
}

func getCount(migrator Migrator, table string) (int, error) {
	res := 0
	query := "select count(*) from " + table
	params := []interface{}{}

	if err := migrator.OldDb.QueryRow(query, params...).Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}
