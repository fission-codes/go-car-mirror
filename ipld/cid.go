package ipld

//import core "github.com/fission-codes/go-car-mirror/carmirror"
import (
	"bytes"
	"errors"
	"io"

	cbor "github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("go-car-mirror")

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

// Read reads the CID from the reader into the Cid.
func (ipfsCid *Cid) Read(reader io.ByteReader) (int, error) {
	var err error
	var i int

	var buf bytes.Buffer

	// This was passed a limited reader, so we can just read until we hit a 0 byte.
	for i = 0; ; i++ {
		var b byte
		if b, err = reader.ReadByte(); err != nil {
			log.Debugw("Read CID error", "err", err)
			return i, err
		}
		if b == 0 {
			log.Debugw("Read CID done", "cid as string", buf.String())
			log.Debugw("Read CID done", "cid as bytes", buf.Bytes())
			break
		}
		buf.WriteByte(b)
	}

	if err = ipfsCid.UnmarshalBinary(buf.Bytes()); err != nil {
		// TODO: Fix this
		// 2022-12-21T21:10:25.034Z        DEBUG   go-car-mirror   ipld/cid.go:76  Read CID done   {"cid as string": "\u0012 y\ufffd'\ufffd\ufffd\u0012\u0008&\u001bB\ufffd<\ufffd\u0007r\ufffd\ufffdܥˌj\ufffd\ufffd\ufffdx\" IHЉ\u00124\n\"\u0012 \ufffd\ufffd\u0006t\ufffd\ufffd\ufffd\u001c\ufffdτ\ufffd\ufffd-խ\ufffd2\ufffd\u001e1\u000fpW9B\u0019\ufffd&I\u000c!\u0012\n1gracrxjpc\u0018\ufffd\ufffd\u0002\u00127\n\"\u0012 \ufffd\ufffd\ufffd<\ufffd\ufffdP\ufffd\u0012\ufffdUx\ufffd\ufffd\ufffd\ufffdt\ufffd\ufffd\ufffdi\ufffd=A\ufffd1\u0011\u0003\ufffd\u0016\u001dh\u0012\r5paz3pdgtf9iy\u0018\ufffd\ufffd\u0003\u00124\n\"\u0012 n\u0011\ufffd`\ufffdIR\ufffd\ufffdç'\ufffd\ufffdש0\u000b\ufffd@\ufffdBT̳\ufffd\ufffd\ufffd\ufffd\u0017\ufffd\u0012\u000b9g14q1sfk0d\u0018\ufffd\u001b\u00121\n\"\u0012 \ufffdN\u001e|!l\ufffd\ufffd\u001b\nz\ufffd:Ð)\ufffd\ufffdml\ufffd\ufffd3\ufffd\ufffd\ufffd\u0017\u0016+\ufffd\u0016\ufffd\u0012\u0008f5zobp0d\u0018\ufffd\u0006\u00120\n\"\u0012 ]ŭOB\ufffd\ufffdf\ufffd\ufffd\u001d\ufffd\ufffd\ufffd\ufffd\ufffdq\ufffd\ufffd\u0019\ufffd^\ufffd><\ufffd\ufffd5g\ufffd\ufffd)\u0012\u0007gpo09zf\u0018\ufffd\u0008\u00129\n\"\u0012 \ufffdj\u0008\u000c}C\ufffdX\ufffdԚ\ufffd\ufffd\ufffdP\u0015ATe\u0011-\ufffd\ufffd\ufffd^Ԭ\u0012\u0004\ufffdU\ufffd\u0012\u000fp6wq52v32_6bbu0\u0018\ufffd\ufffd\u0003\u00129\n\"\u0012 \ufffd\u000b\ufffdgtL\ufffd\ufffd7~Ŋ\ufffd\ufffd\ufffd\n8]/2h\ufffdB\ufffdHOð\u0006\ufffd\u0007\ufffd\u0012\u000frl9d064wqkkf5b3\u0018\ufffd\ufffd\u0003\u00127\n\"\u0012 V!0\ufffd\u0003\ufffd^aP\ufffd\u0005Cg\ufffd\u0010]8\ufffdk\ufffdS\ufffd\ufffd\ufffd"}
		// 2022-12-21T21:10:25.034Z        DEBUG   go-car-mirror   ipld/cid.go:77  Read CID done   {"cid as bytes": "EiB5kSedlxIIJhtC5zyIB3L539yly4xqvt74eCIgSUjQiRI0CiISIKafBnSPgrQcnc+E5tct1a39MrQeMQ9wVzlCGZwmSQwhEgoxZ3JhY3J4anBjGK3zAhI3CiISILOV3jzBnVC3Ep1VeO6x7dF0tbTJacA9QasxEQObFh1oEg01cGF6M3BkZ3RmOWl5GI6eAxI0CiISIG4R4mCNSVK31sOnJ/aT16kwC/lAmEJUzLP4seT+F3+TEgs5ZzE0cTFzZmswZBinGxIxCiISIKxOHnwhbNnQGwp62zrDkCmL+G1sms8z4rPTFxYrhha2EghmNXpvYnAwZBjDBhIwCiISIF3FrU9Cj89m79QdqqiY7bdx4MgZ0l6yPjzntDVn7sYpEgdncG8wOXpmGP8IEjkKIhIguWoIDH1DkVjW1JqO77lQFUFUZREt7ZH8XtSsEgT1VZwSD3A2d3E1MnYzMl82YmJ1MBi54wMSOQoiEiDjC+RndEy51Td+xYqY5OEKOF0vMmirQrVIT8OwBs8HmhIPcmw5ZDA2NHdxa2tmNWIzGLCzAxI3CiISIFYhMNIDv15hUMgFQ2fwEF04qWvtU/bT9w=="}
		// 2022-12-21T21:10:25.034Z        DEBUG   go-car-mirror   messages/messages.go:129        failed to read block id {"error": "trailing bytes in data buffer passed to cid Cast"}
		// 2022-12-21T21:10:25.034Z        ERROR   go-car-mirror   http/server.go:240      parsing blocks message  {"object": "Server", "method": "HandleBlocks", "session": "inLGw79bfHFLHy7JlZRzBQ==", "error": "trailing bytes in data buffer passed to cid Cast"}
		return i, err
	}

	return buf.Len(), nil
}
