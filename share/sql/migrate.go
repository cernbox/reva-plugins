package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
	Expiration                   string
	Permissions                  int
	ShareType                    int
	ShareName                    string
	STime                        int
	FileTarget                   string
	State                        int
	Quicklink                    bool
	Description                  string
	NotifyUploads                bool
	NotifyUploadsExtraRecipients sql.NullString
	Orphan                       bool
}

func oldShareToNewShare(ctx context.Context, migrator Migrator, s OldShareEntry) (*ShareOrLink, error) {
	expirationDate, expirationError := time.Parse("2006-01-02 15:04:05", s.Expiration)

	protoShare := model.ProtoShare{
		Model: gorm.Model{
			ID:        uint(s.ID),
			CreatedAt: time.Unix(int64(s.STime), 0),
			UpdatedAt: time.Unix(int64(s.STime), 0),
		},
		UIDOwner:     s.UIDOwner,
		UIDInitiator: s.UIDInitiator,
		Description:  s.Description,
		Permissions:  uint8(s.Permissions),
		Orphan:       s.Orphan, // will be re-checked later
		Expiration: datatypes.Null[time.Time]{
			V:     expirationDate,
			Valid: expirationError == nil,
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
			protoShare.Orphan = true
		} else {
			// We do not set, because of a general error
			fmt.Printf("An error occured while statting (%s, %s): %s\n", protoShare.Instance, protoShare.Inode, err.Error())
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
			},
		}, nil
	} else if s.ShareType == 3 {
		notifyUploadsExtraRecipients := ""
		if s.NotifyUploadsExtraRecipients.Valid {
			notifyUploadsExtraRecipients = s.NotifyUploadsExtraRecipients.String
		}
		return &ShareOrLink{
			IsShare: false,
			Link: &model.PublicLink{
				ProtoShare:                   protoShare,
				Token:                        s.Token,
				Quicklink:                    s.Quicklink,
				NotifyUploads:                s.NotifyUploads,
				NotifyUploadsExtraRecipients: notifyUploadsExtraRecipients,
				Password:                     s.ShareWith,
				LinkName:                     s.ShareName,
			},
		}, nil
	} else {
		return nil, errors.New("Invalid share type")
	}
}

func getCount(migrator Migrator) (int, error) {
	res := 0
	query := "select count(*) from oc_share"
	params := []interface{}{}

	if err := migrator.OldDb.QueryRow(query, params...).Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}
