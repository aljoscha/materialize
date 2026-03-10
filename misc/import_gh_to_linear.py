#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx"]
# ///
"""
Import GitHub issue trees into Linear as projects.

Usage:
    LINEAR_API_KEY=lin_... uv run misc/import_gh_to_linear.py \
        --team SQL \
        --repo MaterializeInc/database-issues \
        10093 6948

The first issue number becomes the Linear project (title + link to GH issue).
All issue numbers are crawled for sub-issues. The first issue's sub-issues and
all additional issues (with their sub-issue trees) become Linear issues within
that project.
"""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field

import httpx

LINEAR_API_URL = "https://api.linear.app/graphql"


@dataclass
class GHIssue:
    number: int
    title: str
    state: str
    url: str
    children: list["GHIssue"] = field(default_factory=list)


def gh_fetch_issue(repo: str, number: int) -> dict:
    result = subprocess.run(
        [
            "gh", "api", f"repos/{repo}/issues/{number}",
            "--jq", "{number: .number, title: .title, state: .state, url: .html_url}",
        ],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def gh_fetch_sub_issues(repo: str, number: int) -> list[dict]:
    result = subprocess.run(
        [
            "gh", "api", f"repos/{repo}/issues/{number}/sub_issues",
            "--jq", "[.[] | {number: .number, title: .title, state: .state, url: .html_url}]",
        ],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def crawl_issue(repo: str, number: int, visited: set[int]) -> GHIssue | None:
    if number in visited:
        return None
    visited.add(number)

    data = gh_fetch_issue(repo, number)
    issue = GHIssue(
        number=data["number"],
        title=data["title"],
        state=data["state"],
        url=data["url"],
    )

    for sub in gh_fetch_sub_issues(repo, number):
        child = crawl_issue(repo, sub["number"], visited)
        if child:
            issue.children.append(child)

    return issue


def filter_closed(issue: GHIssue) -> None:
    issue.children = [c for c in issue.children if c.state != "closed"]
    for child in issue.children:
        filter_closed(child)


def print_tree(issue: GHIssue, indent: int = 0) -> None:
    prefix = "  " * indent
    marker = "x" if issue.state == "closed" else " "
    print(f"{prefix}[{marker}] #{issue.number}: {issue.title}")
    for child in issue.children:
        print_tree(child, indent + 1)


def count_issues(issues: list[GHIssue]) -> int:
    total = 0
    for issue in issues:
        total += 1 + count_issues(issue.children)
    return total


# --- Linear API ---


def linear_gql(api_key: str, query: str, variables: dict | None = None) -> dict:
    resp = httpx.post(
        LINEAR_API_URL,
        json={"query": query, "variables": variables or {}},
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Linear API error: {json.dumps(data['errors'], indent=2)}")
    return data["data"]


def linear_find_team(api_key: str, team_key: str) -> dict:
    data = linear_gql(api_key, """
        query($filter: TeamFilter) {
            teams(filter: $filter) { nodes { id name key } }
        }
    """, {"filter": {"key": {"eq": team_key}}})
    teams = data["teams"]["nodes"]
    if not teams:
        print(f"Error: no Linear team with key '{team_key}'", file=sys.stderr)
        sys.exit(1)
    return teams[0]


def linear_create_project(api_key: str, team_ids: list[str], name: str, description: str) -> dict:
    data = linear_gql(api_key, """
        mutation($input: ProjectCreateInput!) {
            projectCreate(input: $input) {
                success
                project { id name url }
            }
        }
    """, {"input": {"name": name, "teamIds": team_ids, "description": description}})
    result = data["projectCreate"]
    if not result["success"]:
        raise RuntimeError("Failed to create project")
    return result["project"]


def linear_create_issue(
    api_key: str,
    team_id: str,
    project_id: str,
    title: str,
    description: str,
    parent_id: str | None = None,
) -> dict:
    input_data = {
        "teamId": team_id,
        "projectId": project_id,
        "title": title,
        "description": description,
    }
    if parent_id:
        input_data["parentId"] = parent_id

    data = linear_gql(api_key, """
        mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue { id identifier title url }
            }
        }
    """, {"input": input_data})
    result = data["issueCreate"]
    if not result["success"]:
        raise RuntimeError(f"Failed to create issue: {title}")
    return result["issue"]


def create_issues_recursive(
    api_key: str,
    team_id: str,
    project_id: str,
    issue: GHIssue,
    parent_id: str | None = None,
    indent: int = 0,
) -> None:
    prefix = "  " * indent
    description = f"Imported from GitHub: {issue.url}"

    li = linear_create_issue(api_key, team_id, project_id, issue.title, description, parent_id)
    print(f"{prefix}  Created {li['identifier']}: {li['title']}")
    print(f"{prefix}    -> {li['url']}")

    for child in issue.children:
        create_issues_recursive(api_key, team_id, project_id, child, li["id"], indent + 1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import GitHub issue trees into Linear as projects.",
    )
    parser.add_argument(
        "issues", nargs="+", type=int, metavar="ISSUE",
        help="GitHub issue numbers. First = project, rest = top-level issues to include.",
    )
    parser.add_argument("--repo", default="MaterializeInc/database-issues")
    parser.add_argument("--team", required=True, help="Linear team key (e.g. SQL)")
    parser.add_argument("--include-closed", action="store_true", help="Include closed issues")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be created")
    args = parser.parse_args()

    api_key = os.environ.get("LINEAR_API_KEY", "")
    if not api_key and not args.dry_run:
        print("Error: set LINEAR_API_KEY environment variable", file=sys.stderr)
        sys.exit(1)

    # Crawl all issues, sharing a visited set to avoid duplicates.
    visited: set[int] = set()
    trees: list[GHIssue] = []
    for num in args.issues:
        print(f"Crawling #{num}...")
        tree = crawl_issue(args.repo, num, visited)
        if tree:
            trees.append(tree)

    if not trees:
        print("No issues found.", file=sys.stderr)
        sys.exit(1)

    # First issue becomes the project; its children + remaining trees are the issues.
    project_source = trees[0]
    all_issues: list[GHIssue] = list(project_source.children) + trees[1:]

    if not args.include_closed:
        all_issues = [i for i in all_issues if i.state != "closed"]
        for issue in all_issues:
            filter_closed(issue)

    print(f"\nProject: {project_source.title}")
    print(f"  (from {project_source.url})")
    print(f"\nIssues to create ({count_issues(all_issues)}):")
    for issue in all_issues:
        print_tree(issue, indent=1)

    if args.dry_run:
        print("\n[dry run] Nothing created.")
        return

    print()
    team = linear_find_team(api_key, args.team)
    print(f"Linear team: {team['name']} ({team['key']})")

    project = linear_create_project(
        api_key, [team["id"]],
        project_source.title,
        f"Imported from GitHub: {project_source.url}",
    )
    print(f"Created project: {project['url']}\n")

    for issue in all_issues:
        create_issues_recursive(api_key, team["id"], project["id"], issue)

    print("\nDone!")


if __name__ == "__main__":
    main()
