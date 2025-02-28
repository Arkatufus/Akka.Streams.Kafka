using Google.Protobuf.WellKnownTypes;
using Issue415.Reproduction.Proto;

namespace Issue415.Reproduction;

public sealed record Model(int Sequence, string Data, DateTime Timestamp)
{
    public ModelProto ToProto()
        => new()
        {
            Sequence = Sequence,
            Data = Data,
            Timestamp = Timestamp.ToTimestamp()
        };
    
    public static Model FromProto(ModelProto proto)
        => new(proto.Sequence, proto.Data, proto.Timestamp.ToDateTime());
}