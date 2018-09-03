package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/google/go-github/github"
	_ "github.com/lib/pq" // activates postgres database adapter
)

const schema = `
CREATE TABLE IF NOT EXISTS repos (
	id serial PRIMARY KEY,
	github_owner string NOT NULL,
	github_repo string NOT NULL,
	UNIQUE (github_owner, github_repo)
);

CREATE TABLE IF NOT EXISTS prs (
	id int PRIMARY KEY,
	repo_id int REFERENCES repos,
	number int,
	title string,
	body string,
	open bool,
	merged bool,
	base_sha bytes,
	base_branch string,
	author_username string,
	updated_at timestamptz,
	UNIQUE (repo_id, number)
);

CREATE TABLE IF NOT EXISTS commits (
	id serial PRIMARY KEY,
	sha bytes,
	title string,
	body string,
	author_username string
); `

type repo struct {
	id          int64
	githubOwner string
	githubRepo  string

	releaseBranches []string

	masterCommits    commits
	branchCommits    map[string]commits
	branchMergeBases map[string]sha
}

func (r repo) path() string {
	return filepath.Join("repos", r.githubRepo)
}

func (r repo) url() string {
	return "https://github.com/" + path.Join(r.githubOwner, r.githubRepo) + ".git"
}

func (r *repo) refresh() error {
	cs, err := loadCommits(*r, "master")
	if err != nil {
		return err
	}
	r.masterCommits = cs
	r.branchCommits = map[string]commits{}
	r.branchMergeBases = map[string]sha{}
	for _, branch := range r.releaseBranches {
		cs, err = loadCommits(*r, branch, "^master")
		if err != nil {
			return err
		}
		r.branchCommits[branch] = cs
		out, err := capture("git", "-C", r.path(), "merge-base", "master", branch)
		if err != nil {
			return err
		}
		r.branchMergeBases[branch], err = parseSHA(out)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r repo) ID() int64 {
	return r.id
}

func (r repo) String() string {
	return r.githubOwner + "/" + r.githubRepo
}

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
	if len(args) != 4 {
		return fmt.Errorf("usage: %s <conn-string> <listen-addr> <github-token>", args[0])
	}

	db, err := sql.Open("postgres", args[1])
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}

	ctx := context.Background()
	if err := bootstrap(ctx, db); err != nil {
		return fmt.Errorf("while bootstrapping: %s", err)
	}

	ghClient := github.NewClient(oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: args[3]},
	)))
	go syncLoop(ctx, ghClient, db)

	http.Handle("/", &server{db: db})
	return http.ListenAndServe(args[2], nil)
}

var indexTemplate = template.Must(template.ParseFiles("index.html"))

type server struct {
	db *sql.DB
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Redirect(w, r, "/", http.StatusPermanentRedirect)
		return
	}

	if err := s.serveBoard(w, r); err != nil {
		log.Printf("request handler error: %s", err)
		http.Error(w, "internal error; see logs for details", http.StatusInternalServerError)
		return
	}
}

func (s *server) serveBoard(w http.ResponseWriter, r *http.Request) error {
	var re repo
	if s := r.URL.Query().Get("repo"); s != "" {
		repoID, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		for _, re0 := range repos {
			if re0.id == int64(repoID) {
				re = re0
			}
		}
	} else if len(repos) > 0 {
		re = repos[0]
	} else {
		return errors.New("no repos available")
	}

	var branch string
	if s := r.URL.Query().Get("branch"); s != "" {
		for _, b := range re.releaseBranches {
			if b == s {
				branch = b
			}
		}
		if branch == "" {
			return fmt.Errorf("%q is not a release branch", branch)
		}
	} else if len(re.releaseBranches) > 0 {
		branch = re.releaseBranches[0]
	} else {
		return fmt.Errorf("no release branches for repo %s available", re)
	}

	commits := re.masterCommits.truncate(re.branchMergeBases[branch])

	authors := map[user]struct{}{}
	for _, c := range commits {
		authors[c.Author] = struct{}{}
	}
	var author user
	if s := r.URL.Query().Get("author"); s != "" {
		for a := range authors {
			if a.Email == s {
				author = a
			}
		}
		if author == (user{}) {
			return fmt.Errorf("%q is not a recognized author", author)
		}
		var newCommits []commit
		for _, c := range commits {
			if c.Author == author {
				newCommits = append(newCommits, c)
			}
		}
		commits = newCommits
	}
	var sortedAuthors []user
	for a := range authors {
		sortedAuthors = append(sortedAuthors, a)
	}
	sort.Slice(sortedAuthors, func(i, j int) bool {
		return strings.Compare(sortedAuthors[i].Email, sortedAuthors[j].Email) < 0
	})

	for i := range commits {
		if _, ok := re.branchCommits[branch].messageIDs[commits[i].MessageID()]; ok {
			commits[i].Backported = true
		}
	}

	if err := indexTemplate.Execute(w, struct {
		Repos    []repo
		Repo     repo
		Commits  []commit
		Branches []string
		Branch   string
		Authors  []user
		Author   user
	}{
		Repos:    repos,
		Repo:     re,
		Commits:  commits,
		Branches: re.releaseBranches,
		Branch:   branch,
		Authors:  sortedAuthors,
		Author:   author,
	}); err != nil {
		return err
	}
	return nil
}

func bootstrap(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	for i := range repos {
		var id int64
		if err := db.QueryRowContext(
			ctx,
			`INSERT INTO repos (github_owner, github_repo)
			VALUES ($1, $2)
			ON CONFLICT (github_owner, github_repo) DO UPDATE SET github_owner = excluded.github_owner
			RETURNING id`,
			repos[i].githubOwner, repos[i].githubRepo,
		).Scan(&id); err != nil {
			return err
		}
		repos[i].id = id

		url, path := repos[i].url(), repos[i].path()
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Printf("cloning %s into %s", repos[i], path)
			if err := spawn("git", "clone", "--mirror", url, path); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		out, err := capture("git", "-C", path, "branch", "--list", "release-*")
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(strings.NewReader(out))
		for scanner.Scan() {
			repos[i].releaseBranches = append(repos[i].releaseBranches, strings.TrimSpace(scanner.Text()))
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		sort.Sort(sort.Reverse(sort.StringSlice(repos[i].releaseBranches)))

		if err := repos[i].refresh(); err != nil {
			return err
		}
	}
	return nil
}

func syncLoop(ctx context.Context, ghClient *github.Client, db *sql.DB) {
	for {
		if err := sync(ctx, ghClient, db); err != nil {
			log.Printf("sync error: %s", err)
		}
		<-time.After(time.Minute)
		// TODO: webhook support
	}
}

func sync(ctx context.Context, ghClient *github.Client, db *sql.DB) error {
	for i := range repos {
		if err := syncRepo(ctx, ghClient, db, &repos[i]); err != nil {
			return err
		}
	}
	return nil
}

func syncRepo(ctx context.Context, ghClient *github.Client, db *sql.DB, repo *repo) error {
	log.Printf("syncing %s", repo)
	if err := spawn("git", "-C", repo.path(), "fetch"); err != nil {
		return err
	}
	return nil
	if err := repo.refresh(); err != nil {
		return err
	}

	prs, _, err := ghClient.PullRequests.List(ctx, repo.githubOwner, repo.githubRepo, &github.PullRequestListOptions{
		Sort:        "updated",
		Direction:   "desc",
		ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		return err
	}
	for _, pr := range prs {
		log.Printf("pr: %d", pr.GetNumber())
		if _, err := db.Exec(
			`UPSERT INTO prs (id, repo_id, number, title, body, open, merged, base_sha, base_branch, author_username, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			pr.GetID(), repo.id, pr.GetNumber(),
			pr.GetTitle(), pr.GetBody(),
			pr.GetState() == "open", pr.GetMerged(),
			pr.GetBase().GetSHA(), pr.GetBase().GetRef(),
			pr.GetUser().GetLogin(), pr.GetUpdatedAt(),
		); err != nil {
			return err
		}
	}
	return nil
}

type sha []byte

func (s sha) Short() string {
	return s.String()[0:9]
}

func (s sha) String() string {
	return hex.EncodeToString(s)
}

func parseSHA(s string) (sha, error) {
	shaBytes, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return sha(shaBytes), nil
}

type commits struct {
	commits    []commit
	shas       map[string]struct{}
	messageIDs map[string]struct{}
}

func (cs *commits) insert(c commit) {
	cs.commits = append(cs.commits, c)
	if cs.shas == nil {
		cs.shas = map[string]struct{}{}
	}
	if cs.messageIDs == nil {
		cs.messageIDs = map[string]struct{}{}
	}
	cs.shas[string(c.sha)] = struct{}{}
	cs.messageIDs[c.MessageID()] = struct{}{}
}

func (cs commits) subtract(cs0 commits) []commit {
	var out []commit
	for _, c := range cs.commits {
		if _, ok := cs0.shas[string(c.sha)]; !ok {
			out = append(out, c)
		}
	}
	return out
}

func (cs commits) truncate(sha sha) []commit {
	var out []commit
	for _, cs := range cs.commits {
		if string(cs.sha) == string(sha) {
			break
		}
		if !cs.merge {
			out = append(out, cs)
		}
	}
	return out
}

type user struct {
	Email string
}

func (u user) Short() string {
	return strings.Split(u.Email, "@")[0]
}

func (u user) String() string {
	return u.Email
}

type commit struct {
	sha        sha
	CommitDate time.Time
	Author     user
	title      string
	body       string
	merge      bool
	Backported bool
}

func (c commit) SHA() sha {
	return c.sha
}

func (c commit) Title() string {
	return c.title
}

func (c commit) MessageID() string {
	h := sha1.New()
	io.WriteString(h, c.title)
	io.WriteString(h, c.body)
	return string(h.Sum(nil))
}

const commitFormat = "%H%x00%s%x00%cI%x00%aE%x00%P"

func loadCommits(re repo, constraints ...string) (cs commits, err error) {
	args := []string{
		"git", "-C", re.path(), "log", "--topo-order", "--format=format:" + commitFormat,
	}
	args = append(args, constraints...)
	out, err := capture(args...)
	if err != nil {
		return commits{}, err
	}
	// TODO(benesch): stream this?
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\x00")
		sha, err := parseSHA(fields[0])
		if err != nil {
			return commits{}, err
		}
		commitDate, err := time.Parse(time.RFC3339, fields[2])
		if err != nil {
			return commits{}, err
		}
		authorEmail := fields[3]
		cs.insert(commit{
			sha:        sha,
			CommitDate: commitDate,
			Author:     user{authorEmail},
			title:      fields[1],
			merge:      strings.Count(fields[4], " ") > 0,
		})
	}
	return cs, err
}
