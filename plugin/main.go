package main

import (
	"fmt"

	s3ds "github.com/ipfs/go-ds-s3"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
)

var Plugins = []plugin.Plugin{
	&S3Plugin{},
}

type S3Plugin struct{}

func (s3p S3Plugin) Name() string {
	return "s3-datastore-plugin"
}

func (s3p S3Plugin) Version() string {
	return "0.0.1"
}

func (s3p S3Plugin) Init() error {
	return nil
}

func (s3p S3Plugin) DatastoreTypeName() string {
	return "s3ds"
}

func (s3p S3Plugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		d, ok := m["domain"].(string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no domain specified")
		}

		b, ok := m["bucket"].(string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no bucket specified")
		}

		a, ok := m["accessKey"].(string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no accessKey specified")
		}

		s, ok := m["secretKey"].(string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no secretKey specified")
		}

		return &S3Config{
			cfg: &s3ds.Config{
				Domain:    d,
				Bucket:    b,
				AccessKey: a,
				SecretKey: s,
			},
		}, nil
	}
}

type S3Config struct {
	cfg *s3ds.Config
}

func (s3c *S3Config) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"domain": s3c.cfg.Domain,
		"bucket": s3c.cfg.Bucket,
	}
}

func (s3c *S3Config) Create(path string) (repo.Datastore, error) {
	return s3ds.NewS3Datastore(s3c.cfg), nil
}
