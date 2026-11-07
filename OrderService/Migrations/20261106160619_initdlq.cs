using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OrderService.Migrations
{
    /// <inheritdoc />
    public partial class initdlq : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_OutboxMessages_PublishedAtUtc_OccurredAtUtc",
                table: "OutboxMessages");

            migrationBuilder.AddColumn<DateTime>(
                name: "DeadLetterAtUtc",
                table: "OutboxMessages",
                type: "datetime2",
                nullable: true);

            migrationBuilder.AddColumn<bool>(
                name: "IsDeadLetter",
                table: "OutboxMessages",
                type: "bit",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<string>(
                name: "LockedBy",
                table: "OutboxMessages",
                type: "nvarchar(100)",
                maxLength: 100,
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "LockedUntilUtc",
                table: "OutboxMessages",
                type: "datetime2",
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "NextRetryAtUtc",
                table: "OutboxMessages",
                type: "datetime2",
                nullable: true);

            migrationBuilder.CreateTable(
                name: "ApiIdempotencyRecords",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Operation = table.Column<string>(type: "nvarchar(100)", maxLength: 100, nullable: false),
                    Key = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false, collation: "Latin1_General_100_BIN2"),
                    RequestHash = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    ResourceId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ResponseStatusCode = table.Column<int>(type: "int", nullable: false),
                    CreatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ApiIdempotencyRecords", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessages_PublishedAtUtc_IsDeadLetter_NextRetryAtUtc_OccurredAtUtc",
                table: "OutboxMessages",
                columns: new[] { "PublishedAtUtc", "IsDeadLetter", "NextRetryAtUtc", "OccurredAtUtc" });

            migrationBuilder.CreateIndex(
                name: "UX_ApiIdempotencyRecords_Operation_Key",
                table: "ApiIdempotencyRecords",
                columns: new[] { "Operation", "Key" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ApiIdempotencyRecords");

            migrationBuilder.DropIndex(
                name: "IX_OutboxMessages_PublishedAtUtc_IsDeadLetter_NextRetryAtUtc_OccurredAtUtc",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "DeadLetterAtUtc",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "IsDeadLetter",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "LockedBy",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "LockedUntilUtc",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "NextRetryAtUtc",
                table: "OutboxMessages");

            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessages_PublishedAtUtc_OccurredAtUtc",
                table: "OutboxMessages",
                columns: new[] { "PublishedAtUtc", "OccurredAtUtc" });
        }
    }
}
