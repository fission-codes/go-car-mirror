package ipfs

//import core "github.com/fission-codes/go-car-mirror/carmirror"
import (
	"errors"

	cbor "github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
)

var ErrExpectedByteString = errors.New("expected byte string")
var ErrWrongCborTagNumber = errors.New("wrong CBOR tag for CID")

// The purpose of this is really just to add CBOR serialization/deserialization to the base CID type
type Cid struct{ cid.Cid }

func (ipfsCid Cid) Unwrap() cid.Cid {
	return ipfsCid.Cid
}

func WrapCid(cid cid.Cid) Cid {
	return Cid{cid}
}

func (ipfsCid Cid) MarshalCBOR() ([]byte, error) {
	cid_bytes := make([]byte, 0, ipfsCid.ByteLen()+1)
	cid_bytes = append(cid_bytes, 0)
	cid_bytes = append(cid_bytes, ipfsCid.Bytes()...)

	return cbor.Marshal(cbor.Tag{
		Number:  42,
		Content: cid_bytes,
	})
}

func (ipfsCid *Cid) UnmarshalCBOR(bytes []byte) error {
	tag := cbor.Tag{}
	if err := cbor.Unmarshal(bytes, &tag); err != nil {
		return err
	}
	if tag.Number != 42 {
		return ErrWrongCborTagNumber
	}
	if content, ok := tag.Content.([]byte); !ok {
		return ErrExpectedByteString
	} else {
		ipfsCid.UnmarshalBinary(content[1:])
		return nil
	}
}
