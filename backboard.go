package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"golang.org/x/oauth2"

	"github.com/google/go-github/github"
	_ "github.com/lib/pq" // activate postgres database adapter
)

var repos = []repo{
	{githubOwner: "cockroachdb", githubRepo: "cockroach"},
}

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("usage: %s <conn-string> [<listen-addr>]", args[0])
	}

	githubToken := os.Getenv("BACKBOARD_GITHUB_TOKEN")
	if githubToken == "" {
		return errors.New("missing BACKBOARD_GITHUB_TOKEN env var")
	}

	connString := args[1]
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	if err := bootstrap(ctx, db); err != nil {
		return fmt.Errorf("while bootstrapping: %s", err)
	}

	ghClient := github.NewClient(oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: githubToken},
	)))

	if len(args) == 2 {
		return syncAll(ctx, ghClient, db)
	}
	listenAddr := args[2]
	go syncLoop(ctx, ghClient, db)
	http.Handle("/", &server{db: db})
	return http.ListenAndServe(listenAddr, nil)
}

func syncLoop(ctx context.Context, ghClient *github.Client, db *sql.DB) {
	for {
		if err := syncAll(ctx, ghClient, db); err != nil {
			log.Printf("sync error: %s", err)
		}
		// TODO(benesch): webhook support?
		<-time.After(30 * time.Second)
	}
}
