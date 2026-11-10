using System;
using System.Text.RegularExpressions;
using KnightBus.Core.Outbox;

namespace KnightBus.SqlServer.Outbox;

/// <summary>
/// Settings of the SQL Server outbox, in addition to those of the dispatcher.
/// </summary>
public partial class SqlServerOutboxOptions : OutboxOptions
{
    /// <summary>
    /// Schema of the outbox table.
    /// </summary>
    /// <remarks>Defaults to dbo.</remarks>
    public string Schema { get; set; } = "dbo";

    /// <summary>
    /// Name of the outbox table.
    /// </summary>
    /// <remarks>Defaults to KnightBusOutbox.</remarks>
    public string TableName { get; set; } = "KnightBusOutbox";

    /// <summary>
    /// Whether the schema and table are created when the application starts, if they do not
    /// exist. Turn it off to create them with your own migrations, from
    /// <see cref="SqlServerOutboxSchema.GetCreateScript"/>.
    /// </summary>
    /// <remarks>Defaults to true.</remarks>
    public bool CreateTable { get; set; } = true;

    internal string QualifiedTableName => $"[{Schema}].[{TableName}]";

    public override void Validate()
    {
        base.Validate();
        ValidateIdentifier(Schema, nameof(Schema));
        ValidateIdentifier(TableName, nameof(TableName));
    }

    private static void ValidateIdentifier(string value, string name)
    {
        //The names go into the SQL as they are, so only plain identifiers are allowed
        if (string.IsNullOrEmpty(value) || value.Length > 100 || !Identifier().IsMatch(value))
            throw new ArgumentException(
                $"{name} '{value}' must start with a letter or underscore and contain only letters, digits and underscores",
                name
            );
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Identifier();
}
