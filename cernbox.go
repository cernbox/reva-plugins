package cernbox

import (
	// Add here all the plugins used by cernbox.
	_ "github.com/cernbox/reva-plugins/eosprojects"
	_ "github.com/cernbox/reva-plugins/group"
	_ "github.com/cernbox/reva-plugins/otg"
	_ "github.com/cernbox/reva-plugins/storage/eoshomewrapper"
	_ "github.com/cernbox/reva-plugins/storage/eoswrapper"
	_ "github.com/cernbox/reva-plugins/thumbnails"
	_ "github.com/cernbox/reva-plugins/user"
)