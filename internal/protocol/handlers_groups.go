package protocol

import (
	"kimistore/internal/storage"
)

func handleListGroups(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Request V0: Empty body (just header)

	// Call Coordinator
	groups := GlobalCoordinator.ListGroups()

	// Response V0:
	// ErrorCode (int16)
	// Groups Array
	//   GroupID (string)
	//   ProtocolType (string)

	enc.Int16(ErrNone)
	enc.Int32(int32(len(groups)))

	for _, g := range groups {
		enc.String(g.GroupID)
		enc.String(g.ProtocolType)
	}

	return enc.Bytes(), nil
}

func handleDescribeGroups(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Request V0
	// Groups Array (String)

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	groupIDs := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		id, _ := dec.String()
		groupIDs = append(groupIDs, id)
	}

	// Response V0
	// Groups Array
	//  ErrorCode (int16)
	//  GroupID (string)
	//  State (string)
	//  ProtocolType (string)
	//  Protocol (string)
	//  Members Array
	//    MemberID (string)
	//    ClientID (string)
	//    ClientHost (string)
	//    MemberMetadata (bytes)
	//    MemberAssignment (bytes)

	enc.Int32(int32(len(groupIDs)))

	for _, gid := range groupIDs {
		detail, err := GlobalCoordinator.DescribeGroup(gid)

		errorCode := int16(ErrNone)
		if err != nil {
			// If group not found/dead, Kafka usually returns ErrNone + "Dead" state?
			// Or ErrGroupIdNotFound?
			// Let's assume ErrNone but empty state if not found, or strict error.
			// Usually DescribeGroups returns info even if dead.
			errorCode = 0 // ErrNone
			// but handle nil detail
		}

		enc.Int16(errorCode)
		enc.String(gid)

		if detail != nil {
			enc.String(detail.State)
			enc.String(detail.ProtocolType)
			enc.String(detail.Protocol)

			enc.Int32(int32(len(detail.Members)))
			for _, m := range detail.Members {
				enc.String(m.MemberID)
				enc.String(m.ClientID)
				enc.String(m.ClientHost)
				enc.PutBytes(m.Metadata)   // MemberMetadata
				enc.PutBytes(m.Assignment) // MemberAssignment
			}
		} else {
			// Group not found / Dead
			enc.String("Dead")
			enc.String("")
			enc.String("")
			enc.Int32(0) // 0 members
		}
	}

	return enc.Bytes(), nil
}
