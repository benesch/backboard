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

var indexTemplate = template.Must(template.New("index.html").Parse(`<!doctype html>
<html>
<head>
    <style>
        body {
            font-family: helvetica, sans-serif;
            font-size: 14px;
        }

        h1 {
            margin: 0 0 10px;
		}

		h1 a {
			color: inherit;
			text-decoration: none;
		}

        label {
            color: #666;
            font-size: 13px;
            letter-spacing: 1px;
            text-transform: uppercase;
        }

        .header {
            margin: 0 auto;
            text-align: center;
        }

        .forms {
            margin: 0 auto;
            text-align: left;
            width: 400px;
        }

        .forms span {
            display: inline-block;
            text-align: right;
            width: 135px;
        }

        .forms select {
            max-width: 200px;
        }

        .commit-table {
            border-collapse: collapse;
            margin: 1em auto 0;
            padding: 0.1em;
        }

		.commit-table tr.master-border td.master-border,
		.commit-table tr.backport-border td.backport-border {
            border-top: 1px solid #bbb;
		}

        /*
        .commit-table tr {
            cursor: pointer;
        }

        .commit-table tbody tr:hover td:nth-child(-n + 4) {
            background: #fffbcc;
        }
        */

        .commit-table td {
            padding: 0.3em 0.3em;
        }

        .commit-table form {
            visibility: hidden;
        }

        .commit-table tr:hover form {
            visibility: visible;
        }

        .sha {
            font-family: monospace;
        }

        .center {
            text-align: center;
        }
    </style>
    <title>backboard</title>
</head>
<body>
<div class="header">
    <h1><a href="/">backboard</a></h1>
    <div class="forms">
        <form>
            <label>
                <span>repo</span>
                <select name="repo">
                {{range .Repos}}
                    <option value="{{.ID}}">{{.}}</option>
                {{end}}
                </select>
                <input type="submit" value="go">
            </label>
        </form>
        <form>
            <label>
                <span>branch</span>
                <select name="branch">
                    {{range .Branches}}
                        <option {{if eq . $.Branch}}selected{{end}}>{{.}}</option>
                    {{end}}
                </select>
                <input type="hidden" name="repo" value="{{.Repo.ID}}">
                <input type="submit" value="go">
            </label>
        </form>
        <form>
            <label>
                <span>author</span>
                <select name="author">
                    <option value="">All authors</option>
                    {{range .Authors}}
                        <option {{if eq $.Author.Email .Email}}selected{{end}}>{{.}}</option>
                    {{end}}
                </select>
                <input type="hidden" name="repo" value="{{.Repo.ID}}">
                <input type="hidden" name="branch" value="{{.Branch}}">
                <input type="submit" value="go">
            </label>
        </form>
        <!--<form>
            <label>
                <span>show excluded</span>
                <input type="checkbox" name="excluded">
                <input type="submit" value="go">
            </label>
        </form>-->
    </div>
</div>
<table class="commit-table">
    <thead>
    <tr>
        <th>SHA</th>
        <th>Date</th>
        <th>Author</th>
        <th>Title</th>
        <th>MPR</th>
        <th>BPR</th>
        <th>Ok?</th>
        <th></th>
    </tr>
    </thead>
    <tbody>
    {{range .Commits}}
        <tr class="{{if .MasterPRRowSpan}}master-border{{end}} {{if .BackportPRRowSpan}}backport-border{{end}}">
            <td class="sha master-border" title="{{.SHA}}">{{.SHA.Short}}</td>
            <td class="master-border">{{.CommitDate.Format "2006-01-02"}}</td>
            <td class="master-border" title="{{.Author.Email}}">{{.Author.Short}}</td>
            <td class="master-border">{{.Title}}</td>
            {{if .MasterPRRowSpan}}
                <td class="master-border" rowspan="{{.MasterPRRowSpan}}"><a href="{{.MasterPR.URL}}">{{.MasterPR}}</a></td>
			{{end}}
			{{if .BackportPRRowSpan}}
				<td class="backport-border" rowspan="{{.BackportPRRowSpan}}"><a href="{{.BackportPR.URL}}">{{.BackportPR}}</a></td>
			{{end}}
            <td class="backport-border center">{{.BackportStatus}}</td>
        </tr>
    {{end}}
    </tbody>
</table>
</body>
</html>`))

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
	repoLock.RLock()
	defer repoLock.RUnlock()

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
	var lastBackportPR *pr
	backportPRStart := -1
	for i, c := range commits {
		// TODO(benesch): these rowspan computations hurt to look at.
		masterPR := re.masterPRs[string(c.sha)]
		// TODO(benesch): masterPR should never be nil!
		if masterPR != nil && (lastMasterPR == nil || lastMasterPR.number != masterPR.number) {
			if masterPRStart >= 0 {
				acommits[masterPRStart].MasterPRRowSpan = i - masterPRStart
			}
			masterPRStart = i
			lastMasterPR = masterPR
		}
		backportPR := re.branchPRs[c.MessageID()][branch]
		if !((lastBackportPR == nil && backportPR == nil && lastMasterPR != masterPR) || (lastBackportPR != nil && backportPR != nil && lastBackportPR.number == backportPR.number)) {
			if backportPRStart >= 0 {
				acommits[backportPRStart].BackportPRRowSpan = i - backportPRStart
			}
			backportPRStart = i
			lastBackportPR = backportPR
		}

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
	if masterPRStart >= 0 {
		acommits[masterPRStart].MasterPRRowSpan = len(acommits) - masterPRStart
	}
	if backportPRStart >= 0 {
		acommits[backportPRStart].BackportPRRowSpan = len(acommits) - backportPRStart
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
