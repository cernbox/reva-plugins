package share

import (
	"strconv"
	"time"

	grouppb "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	resourcespb "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	link "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typespb "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	conversions "github.com/cs3org/reva/pkg/cbox/utils"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// +---------------------------------+-----------------+------+-----+---------+----------------+
// | Field                           | Type            | Null | Key | Default | Extra          |
// +---------------------------------+-----------------+------+-----+---------+----------------+
// | id                              | int             | NO   | PRI | NULL    | auto_increment |
// | share_type                      | smallint        | NO   |     | 0       |                |
// | share_with                      | varchar(255)    | YES  |     | NULL    |                |
// | uid_owner                       | varchar(64)     | NO   |     |         |                |
// | uid_initiator                   | varchar(64)     | YES  |     | NULL    |                |
// | parent                          | int             | YES  |     | NULL    |                |
// | item_type                       | varchar(64)     | NO   | MUL |         |                |
// | item_source                     | varchar(255)    | YES  |     | NULL    |                |
// | item_target                     | varchar(255)    | YES  |     | NULL    |                |
// | file_source                     | bigint unsigned | YES  | MUL | NULL    |                |
// | file_target                     | varchar(512)    | YES  |     | NULL    |                |
// | permissions                     | smallint        | NO   |     | 0       |                |
// | stime                           | bigint          | NO   |     | 0       |                |
// | accepted                        | smallint        | NO   |     | 0       |                |
// | expiration                      | datetime        | YES  |     | NULL    |                |
// | token                           | varchar(32)     | YES  | MUL | NULL    |                |
// | mail_send                       | smallint        | NO   |     | 0       |                |
// | fileid_prefix                   | varchar(255)    | YES  |     | NULL    |                |
// | orphan                          | tinyint         | YES  |     | NULL    |                |
// | share_name                      | varchar(255)    | YES  |     | NULL    |                |
// | quicklink                       | tinyint(1)      | NO   |     | 0       |                |
// | description                     | varchar(1024)   | NO   |     |         |                |
// | internal                        | tinyint(1)      | NO   | MUL | 0       |                |
// | notify_uploads                  | tinyint(1)      | NO   |     | 0       |                |
// | notify_uploads_extra_recipients | varchar(2048)   | YES  |     | NULL    |                |
// +---------------------------------+-----------------+------+-----+---------+----------------+

type protoShare struct {
	// Including gorm.Model will embed a number of gorm-default fields
	// such as creation_time, id etc
	gorm.Model
	UIDOwner     string
	UIDInitiator string
	ItemType     string // file | folder
	InitialPath  string
	Inode        string
	FileSource   int64
	FileTarget   string
	Permissions  uint8
	Instance     string
	Orphan       bool
	Description  string
	Expiration   datatypes.Null[datatypes.Date]
}

type Share struct {
	protoShare
	ShareWith         string
	SharedWithIsGroup bool
}

type PublicLink struct {
	protoShare
	Token string
	// Enforce uniqueness in db re: Itemsource
	Quicklink                    bool
	NotifyUploads                bool
	NotifyUploadsExtraRecipients string
	Password                     string
	// Users can give a name to a share
	ShareName string
}

// Unique index on combo of (shareid, user)
type ShareState struct {
	gorm.Model
	ShareId uint //foreign key to share
	// Can not be uid because of lw accs
	User   string
	Synced bool
	Hidden bool
}

func (s *Share) AsCS3Share(granteeType userpb.UserType) *collaboration.Share {
	ts := &typespb.Timestamp{
		Seconds: uint64(s.CreatedAt.Unix()),
	}
	return &collaboration.Share{
		Id: &collaboration.ShareId{
			OpaqueId: strconv.Itoa(int(s.ID)),
		},
		//ResourceId:  &provider.Reference{StorageId: s.Prefix, NodeId: s.ItemSource},
		ResourceId: &provider.ResourceId{
			StorageId: s.Instance,
			OpaqueId:  s.Inode,
		},
		Permissions: &collaboration.SharePermissions{Permissions: conversions.IntTosharePerm(int(s.Permissions), s.ItemType)},
		Grantee:     extractGrantee(s.SharedWithIsGroup, s.ShareWith, granteeType),
		Owner:       conversions.MakeUserID(s.UIDOwner),
		Creator:     conversions.MakeUserID(s.UIDInitiator),
		Ctime:       ts,
		Mtime:       ts,
	}
}

func (s *Share) AsCS3ReceivedShare(state *ShareState, granteeType userpb.UserType) *collaboration.ReceivedShare {
	return &collaboration.ReceivedShare{
		Share:  s.AsCS3Share(granteeType),
		State:  resourcespb.ShareState_SHARE_STATE_ACCEPTED,
		Hidden: state.Hidden,
	}
}

func (p *PublicLink) AsCS3PublicShare() *link.PublicShare {
	ts := &typespb.Timestamp{
		Seconds: uint64(p.CreatedAt.Unix()),
	}
	pwd := false
	if p.Password != "" {
		pwd = true
	}
	var expires *typespb.Timestamp
	if p.Expiration.Valid {
		exp, err := p.Expiration.V.Value()
		expiration := exp.(time.Time)
		if err == nil {
			expires = &typespb.Timestamp{
				Seconds: uint64(expiration.Unix()),
			}
		}

	}
	return &link.PublicShare{
		Id: &link.PublicShareId{
			OpaqueId: strconv.Itoa(int(p.ID)),
		},
		ResourceId: &provider.ResourceId{
			StorageId: p.Instance,
			OpaqueId:  p.Inode,
		},
		Permissions:                  &link.PublicSharePermissions{Permissions: conversions.IntTosharePerm(int(p.Permissions), p.ItemType)},
		Owner:                        conversions.MakeUserID(p.UIDOwner),
		Creator:                      conversions.MakeUserID(p.UIDInitiator),
		Token:                        p.Token,
		DisplayName:                  p.ShareName,
		PasswordProtected:            pwd,
		Expiration:                   expires,
		Ctime:                        ts,
		Mtime:                        ts,
		Quicklink:                    p.Quicklink,
		Description:                  p.Description,
		NotifyUploads:                p.NotifyUploads,
		NotifyUploadsExtraRecipients: p.NotifyUploadsExtraRecipients,
	}
}

// The package 'conversions' is currently internal in Reva
// It should become public so we can use it here
// Since it generates CS3ResourcePermissions I'm not sure why it would be private

// IntTosharePerm retrieves read/write permissions from an integer.
func intTosharePerm(p int, itemType string) *provider.ResourcePermissions {
	switch p {
	case 1:
		return conversions.NewViewerRole().CS3ResourcePermissions()
	case 15:
		if itemType == "folder" {
			return conversions.NewEditorRole().CS3ResourcePermissions()
		}
		return conversions.NewFileEditorRole().CS3ResourcePermissions()
	case 4:
		return conversions.NewUploaderRole().CS3ResourcePermissions()
	default:
		// TODO we may have other options, for now this is a denial
		return &provider.ResourcePermissions{}
	}
}

// ExtractGrantee retrieves the CS3API Grantee from a grantee type and username/groupname.
// The grantee userType is relevant only for users.
func extractGrantee(sharedWithIsGroup bool, g string, gtype userpb.UserType) *provider.Grantee {
	var grantee provider.Grantee
	if sharedWithIsGroup {
		grantee.Type = provider.GranteeType_GRANTEE_TYPE_USER
		grantee.Id = &provider.Grantee_UserId{UserId: &userpb.UserId{
			OpaqueId: g,
			Type:     gtype,
		}}
	} else {
		grantee.Type = provider.GranteeType_GRANTEE_TYPE_GROUP
		grantee.Id = &provider.Grantee_GroupId{GroupId: &grouppb.GroupId{
			OpaqueId: g,
		}}
	}

	return &grantee
}
