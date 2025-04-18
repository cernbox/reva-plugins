module github.com/cernbox/reva-plugins

go 1.22.7

require (
	github.com/Masterminds/sprig v2.22.0+incompatible
	github.com/bluele/gcache v0.0.2
	github.com/cs3org/go-cs3apis v0.0.0-20250218144737-544dd3919658
	github.com/cs3org/reva v1.27.0
	github.com/disintegration/imaging v1.6.2
	github.com/go-chi/chi/v5 v5.2.1
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gomodule/redigo v1.9.2
	github.com/juliangruber/go-intersect v1.1.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.33.0
	golang.org/x/crypto v0.32.0
	google.golang.org/genproto v0.0.0-20241209162323-e6fa225c2576
	google.golang.org/grpc v1.71.0
	gorm.io/datatypes v1.2.4
	gorm.io/driver/mysql v1.5.7
	gorm.io/driver/sqlite v1.5.7
	gorm.io/gorm v1.25.12
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20221128193559-754e69321358 // indirect
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/ReneKroon/ttlcache/v2 v2.11.0 // indirect
	github.com/cern-eos/go-eosgrpc v0.0.0-20240909164147-ad693be93181 // indirect
	github.com/creasty/defaults v1.8.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/glpatcern/go-mime v0.0.0-20221026162842-2a8d71ad17a9 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.7 // indirect
	github.com/go-ldap/ldap/v3 v3.4.10 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.25.0 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-sqlite3 v1.14.24 // indirect
	github.com/mileusna/useragent v1.3.5 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	go.step.sm/crypto v0.57.0 // indirect
	golang.org/x/image v0.13.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace github.com/cs3org/reva => ../reva
