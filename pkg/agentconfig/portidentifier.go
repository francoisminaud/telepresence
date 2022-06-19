package agentconfig

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// PortIdentifier identifies a port (service or container) unambiguously using
// the notation <name or number>/<protocol>. A named port will always be identified
// using the name and the protocol will only be appended when it is not TCP.
type PortIdentifier string

var ErrNotInteger = errors.New("not an integer")

const ProtoSeparator = byte('/')

// ParseNumericPort parses the given string into a positive unsigned 16-bit integer.
// ErrNotInteger is returned if the string doesn't represent an integer.
// A range error is return unless the integer is between 1 and 65535
func ParseNumericPort(portStr string) (uint16, error) {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, ErrNotInteger
	}
	if port < 1 || port > math.MaxUint16 {
		return 0, fmt.Errorf("%s is not between 1 and 65535", portStr)
	}
	return uint16(port), nil
}

// ValidatePort validates a port string. An error is returned if the string isn't a
// number between 1 and 65535 or a DNS_LABEL
func ValidatePort(s string) error {
	_, err := ParseNumericPort(s)
	if err == ErrNotInteger {
		err = nil
		if errs := validation.IsDNS1035Label(s); len(errs) > 0 {
			err = fmt.Errorf(strings.Join(append([]string{err.Error()}, errs...), " and "))
		}
	}
	return err
}

// NewPortIdentifier creates a new PortIdentifier from a protocol and a string that
// is either a name or a number. An error is returned if the protocol is unsupported,
// if a port number is not between 1 and 65535, or if the name isn't a DNS_LABEL
func NewPortIdentifier(protocol string, portString string) (PortIdentifier, error) {
	if err := ValidatePort(portString); err != nil {
		return "", err
	}
	if protocol != "" {
		pr, err := ParseProtocol(protocol)
		if err != nil {
			return "", err
		}
		portString += string([]byte{ProtoSeparator}) + string(pr)
	}
	return PortIdentifier(portString), nil
}

func ParseProtocol(protocol string) (core.Protocol, error) {
	pr := core.Protocol(strings.ToUpper(protocol))
	switch pr {
	case "":
		return core.ProtocolTCP, nil
	case core.ProtocolUDP, core.ProtocolTCP:
		return pr, nil
	default:
		return "", fmt.Errorf("unsupported protocol: %s", pr)
	}
}

// HasProto returns the protocol, and the name or number.
func (spi PortIdentifier) HasProto() bool {
	return strings.IndexByte(string(spi), ProtoSeparator) > 0
}

// ProtoAndNameOrNumber returns the protocol, and the name or number.
func (spi PortIdentifier) ProtoAndNameOrNumber() (core.Protocol, string, uint16) {
	s := string(spi)
	p := core.ProtocolTCP
	if ix := strings.IndexByte(s, ProtoSeparator); ix > 0 {
		p = core.Protocol(s[ix+1:])
		s = s[0:ix]
	}
	if n, err := strconv.Atoi(s); err == nil {
		return p, "", uint16(n)
	}
	return p, s, 0
}

func (spi PortIdentifier) String() string {
	return string(spi)
}
