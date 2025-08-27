package cernbox

import (
	// Add here all the plugins used by cernbox.
	_ "github.com/cernbox/reva-plugins/cback/http"
	_ "github.com/cernbox/reva-plugins/cback/storage"
	_ "github.com/cernbox/reva-plugins/cernboxspaces"
	_ "github.com/cernbox/reva-plugins/eosprojects"
	_ "github.com/cernbox/reva-plugins/group"
	_ "github.com/cernbox/reva-plugins/otg"
	_ "github.com/cernbox/reva-plugins/share/sql"
	_ "github.com/cernbox/reva-plugins/storage/eoswrapper"
	_ "github.com/cernbox/reva-plugins/thumbnails"
	_ "github.com/cernbox/reva-plugins/user"
)
