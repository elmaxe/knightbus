using KnightBus.Core.Exceptions;

namespace KnightBus.Core.Outbox;

/// <summary>
/// A message was written to the outbox without an open transaction enlisted in the
/// <see cref="IOutboxSession"/>. Without one the message would not be part of the caller's
/// transaction, so it is refused rather than sent or stored on its own.
/// </summary>
public class OutboxNotEnlistedException : KnightBusException
{
    public OutboxNotEnlistedException()
        : base(
            "No open transaction is enlisted in the outbox session. Enlist the transaction the "
                + "messages should be part of before writing them, for example with "
                + "IOutboxSession.UseSqlTransaction(transaction) from KnightBus.SqlServer"
        ) { }
}
