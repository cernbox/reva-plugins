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

type ItemType string

const (
	ItemTypeFile      ItemType = "file"
	ItemTypeFolder    ItemType = "folder"
	ItemTypeReference ItemType = "reference"
	ItemTypeSymlink   ItemType = "symlink"
)

func (i ItemType) String() string {
	return string(i)
}

type ProtoShare struct {
	// Including gorm.Model will embed a number of gorm-default fields
	gorm.Model
	UIDOwner     string
	UIDInitiator string
	ItemType     ItemType // file | folder | reference | symlink
	InitialPath  string
	Inode        string
	Instance     string
	Permissions  uint8
	Orphan       bool
	Expiration   datatypes.NullTime
}

type Share struct {
	ProtoShare
	ShareWith         string
	SharedWithIsGroup bool
	Description       string
}

type PublicLink struct {
	ProtoShare
	Token string
	// Enforce uniqueness in db re: Itemsource
	Quicklink                    bool
	NotifyUploads                bool
	NotifyUploadsExtraRecipients string
	Password                     string
	// Users can give a name to a share
	LinkName string
}

type ShareState struct {
	gorm.Model
	ShareID uint  `gorm:"foreignKey:ShareID;references:ID"` // Define the foreign key field
	Share   Share // Define the association
	// Can not be uid because of lw accs
	User   string
	Synced bool
	Hidden bool
	Alias  string
}

func (s *Share) AsCS3Share(granteeType userpb.UserType) *collaboration.Share {
	creationTs := &typespb.Timestamp{
		Seconds: uint64(s.CreatedAt.Unix()),
	}
	updateTs := &typespb.Timestamp{
		Seconds: uint64(s.UpdatedAt.Unix()),
	}
	return &collaboration.Share{
		Id: &collaboration.ShareId{
			OpaqueId: strconv.FormatUint(uint64(s.ID), 10),
		},
		//ResourceId:  &provider.Reference{StorageId: s.Prefix, NodeId: s.ItemSource},
		ResourceId: &provider.ResourceId{
			StorageId: s.Instance,
			OpaqueId:  s.Inode,
		},
		Permissions: &collaboration.SharePermissions{Permissions: conversions.IntTosharePerm(int(s.Permissions), s.ItemType.String())},
		Grantee:     extractGrantee(s.SharedWithIsGroup, s.ShareWith, granteeType),
		Owner:       conversions.MakeUserID(s.UIDOwner),
		Creator:     conversions.MakeUserID(s.UIDInitiator),
		Ctime:       creationTs,
		Mtime:       updateTs,
		Description: s.Description,
	}
}

func (s *Share) AsCS3ReceivedShare(state *ShareState, granteeType userpb.UserType) *collaboration.ReceivedShare {
	// Currently, some implementations still rely on the ShareState to determine whether a file is hidden
	// instead of using the field
	var rsharestate resourcespb.ShareState
	if state.Hidden {
		rsharestate = resourcespb.ShareState_SHARE_STATE_REJECTED
	} else {
		rsharestate = resourcespb.ShareState_SHARE_STATE_ACCEPTED
	}

	return &collaboration.ReceivedShare{
		Share:  s.AsCS3Share(granteeType),
		State:  rsharestate,
		Hidden: state.Hidden,
		Alias:  state.Alias,
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
		exp, err := p.Expiration.Value()
		if err == nil {
			expiration := exp.(time.Time)
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
		Permissions:                  &link.PublicSharePermissions{Permissions: conversions.IntTosharePerm(int(p.Permissions), p.ItemType.String())},
		Owner:                        conversions.MakeUserID(p.UIDOwner),
		Creator:                      conversions.MakeUserID(p.UIDInitiator),
		Token:                        p.Token,
		DisplayName:                  p.LinkName,
		PasswordProtected:            pwd,
		Expiration:                   expires,
		Ctime:                        ts,
		Mtime:                        ts,
		Quicklink:                    p.Quicklink,
		NotifyUploads:                p.NotifyUploads,
		NotifyUploadsExtraRecipients: p.NotifyUploadsExtraRecipients,
	}
}

// ExtractGrantee retrieves the CS3API Grantee from a grantee type and username/groupname.
// The grantee userType is relevant only for users.
func extractGrantee(sharedWithIsGroup bool, g string, gtype userpb.UserType) *provider.Grantee {
	var grantee provider.Grantee
	if sharedWithIsGroup {
		grantee.Type = provider.GranteeType_GRANTEE_TYPE_GROUP
		grantee.Id = &provider.Grantee_GroupId{GroupId: &grouppb.GroupId{
			OpaqueId: g,
		}}
	} else {
		grantee.Type = provider.GranteeType_GRANTEE_TYPE_USER
		grantee.Id = &provider.Grantee_UserId{UserId: &userpb.UserId{
			OpaqueId: g,
			Type:     gtype,
		}}
	}
	return &grantee
}
