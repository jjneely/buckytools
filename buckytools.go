package buckytools

const (
	// Buckytools suite version
	Version = "0.4.0"
)

// SupportedHashTypes is the string identifiers of the hashing algorithms
// used for the consistent hash ring.  This slice must be sorted.
var SupportedHashTypes = []string{
	"carbon",
	"fnv1a",
	"jump_fnv1a",
}
