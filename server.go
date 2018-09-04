package main

import (
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

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

	var acommits []acommit
	var lastMasterPR *pr
	masterPRStart := -1
	for i, c := range commits {
		masterPR := re.masterPRs[string(c.sha)]
		// TODO(benesch): masterPR should never be nil!
		if masterPR != nil {
			if lastMasterPR != nil && lastMasterPR.number == masterPR.number {
				masterPR = nil
			} else {
				if masterPRStart >= 0 {
					acommits[masterPRStart].MasterPRRowSpan = i - masterPRStart
				}
				masterPRStart = i
				lastMasterPR = masterPR
			}
		}
		backportPR := re.branchPRs[c.MessageID()][branch]
		var backportStatus string
		if backportPR != nil {
			if backportPR.merged {
				backportStatus = "✓"
			} else {
				backportStatus = "◷"
			}
		}
		// TODO(benesch): redundant. which to keep?
		if _, backported := re.branchCommits[branch].messageIDs[c.MessageID()]; backported {
			backportStatus = "✓"
		}
		acommits = append(acommits, acommit{
			commit:         c,
			BackportStatus: backportStatus,
			MasterPR:       masterPR,
			BackportPR:     backportPR,
		})
	}

	if err := indexTemplate.Execute(w, struct {
		Repos    []repo
		Repo     repo
		Commits  []acommit
		Branches []string
		Branch   string
		Authors  []user
		Author   user
	}{
		Repos:    repos,
		Repo:     re,
		Commits:  acommits,
		Branches: re.releaseBranches,
		Branch:   branch,
		Authors:  sortedAuthors,
		Author:   author,
	}); err != nil {
		return err
	}
	return nil
}
