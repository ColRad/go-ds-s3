package s3ds

import (
	"testing"

	s3 "gx/ipfs/QmR1J2FeuQqVZwigFA8e1NPXWacgKGZ3uV8ikqzHua6Pm3/s3gof3r"
	dstest "gx/ipfs/QmeiCcJfDW1GJnWUArudsv5rQsihpi4oyddPhdqo3CfX6i/go-datastore/test"
)

func TestSuite(t *testing.T) {
	s3ds := createDstore(t)
	t.Run("basic operations", func(t *testing.T) {
		dstest.SubtestBasicPutGet(t, s3ds)
	})
	t.Run("not found operations", func(t *testing.T) {
		dstest.SubtestNotFounds(t, s3ds)
	})
	t.Run("many puts and gets, query", func(t *testing.T) {
		dstest.SubtestManyKeysAndQuery(t, s3ds)
	})
}

func createDstore(t *testing.T) *S3Bucket {
	keys, err := s3.EnvKeys()
	if err != nil {
		t.Fatal(err)
	}

	s3c := s3.New("", keys)
	buck := s3c.Bucket("whytesting")
	return &S3Bucket{
		s3c:  s3c,
		buck: buck,
	}
}
