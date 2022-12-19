package ipld

//import core "github.com/fission-codes/go-car-mirror/carmirror"
import (
	"errors"

	cbor "github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
)

var ErrExpectedByteString = errors.New("expected byte string")
var ErrWrongCborTagNumber = errors.New("wrong CBOR tag for CID")

// Cid wraps cid.Cid, in order to add CBOR serialization and deserialization.
type Cid struct{ cid.Cid }

// Unwrap returns the underlying cid.Cid.
func (ipfsCid Cid) Unwrap() cid.Cid {
	return ipfsCid.Cid
}

// WrapCid wraps a cid.Cid in a Cid.
func WrapCid(cid cid.Cid) Cid {
	return Cid{cid}
}

// MarshalCBOR implements the CBOR marshaler interface.
func (ipfsCid Cid) MarshalCBOR() ([]byte, error) {
	cidBytes := make([]byte, 0, ipfsCid.ByteLen()+1)
	cidBytes = append(cidBytes, 0)
	cidBytes = append(cidBytes, ipfsCid.Bytes()...)

	return cbor.Marshal(cbor.Tag{
		Number:  42,
		Content: cidBytes,
	})
}

// UnmarshalCBOR implements the CBOR unmarshaler interface.
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