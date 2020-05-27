package server

import (
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
)

func NewObjClient(conf *serviceenv.Configuration) (objClient obj.Client, err error) {
	var (
		dir = conf.StorageRoot
	)
	switch conf.StorageBackend {
	case MinioBackendEnvVar:
		// S3 compatible doesn't like leading slashes
		if len(dir) > 0 && dir[0] == '/' {
			dir = dir[1:]
		}
		objClient, err = obj.NewMinioClientFromSecret(dir)

	case AmazonBackendEnvVar:
		// amazon doesn't like leading slashes
		if len(dir) > 0 && dir[0] == '/' {
			dir = dir[1:]
		}
		objClient, err = obj.NewAmazonClientFromSecret(dir)

	case GoogleBackendEnvVar:
		// TODO figure out if google likes leading slashses
		objClient, err = obj.NewGoogleClientFromSecret(dir)

	case MicrosoftBackendEnvVar:
		objClient, err = obj.NewMicrosoftClientFromSecret(dir)

	case LocalBackendEnvVar:
		fallthrough
	default:
		objClient, err = obj.NewLocalClient(dir)
	}
	return objClient, err
}
