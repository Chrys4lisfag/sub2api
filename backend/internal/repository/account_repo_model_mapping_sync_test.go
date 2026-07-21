package repository

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestAccountRepositorySyncAntigravityDefaultModelMappings(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`(?s)WITH candidates AS MATERIALIZED.*UPDATE accounts AS a.*jsonb_set`).
		WithArgs(`{"gemini-a":"gemini-a","gemini-b":"wire-b"}`).
		WillReturnRows(sqlmock.NewRows([]string{
			"scanned_accounts",
			"eligible_accounts",
			"updated_accounts",
			"unchanged_accounts",
			"skipped_accounts",
			"added_mappings",
			"updated_ids",
		}).AddRow(7, 5, 2, 3, 2, 4, "{11,12}"))
	mock.ExpectExec(`INSERT INTO scheduler_outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	repo := newAccountRepositoryWithSQL(nil, db, nil)
	result, err := repo.SyncAntigravityDefaultModelMappings(context.Background(), map[string]string{
		"gemini-a": "gemini-a",
		"gemini-b": "wire-b",
	})

	require.NoError(t, err)
	require.EqualValues(t, 7, result.ScannedAccounts)
	require.EqualValues(t, 5, result.EligibleAccounts)
	require.EqualValues(t, 2, result.UpdatedAccounts)
	require.EqualValues(t, 3, result.UnchangedAccounts)
	require.EqualValues(t, 2, result.SkippedAccounts)
	require.EqualValues(t, 4, result.AddedMappings)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAccountRepositorySyncAntigravityDefaultModelMappingsEmptyDefaults(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repo := newAccountRepositoryWithSQL(nil, db, nil)
	result, err := repo.SyncAntigravityDefaultModelMappings(context.Background(), nil)

	require.NoError(t, err)
	require.Zero(t, result.ScannedAccounts)
	require.NoError(t, mock.ExpectationsWereMet())
}
