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

Each GitHub issue number is crawled for sub-issues. Every issue found
(roots + all transitive sub-issues) becomes a Linear project under the
given team, with its description linking back to the original GitHub issue.
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


def flatten(issue: GHIssue) -> list[GHIssue]:
    """Flatten an issue tree into a list (pre-order)."""
    result = [issue]
    for child in issue.children:
        result.extend(flatten(child))
    return result


def print_tree(issue: GHIssue, indent: int = 0) -> None:
    prefix = "  " * indent
    marker = "x" if issue.state == "closed" else " "
    print(f"{prefix}[{marker}] #{issue.number}: {issue.title}")
    for child in issue.children:
        print_tree(child, indent + 1)


MAX_PROJECT_NAME_LEN = 80


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


def linear_fetch_existing_projects(api_key: str, team_id: str) -> dict[str, dict]:
    """Fetch all existing projects for a team. Returns {name: project} mapping.
    Each project includes an 'external_link_urls' set for idempotency checks."""
    projects: dict[str, dict] = {}
    has_more = True
    cursor = None
    while has_more:
        if cursor:
            query = """
                query($filter: ProjectFilter, $after: String) {
                    projects(filter: $filter, after: $after, first: 100) {
                        nodes { id name url externalLinks { nodes { url } } }
                        pageInfo { hasNextPage endCursor }
                    }
                }
            """
        else:
            query = """
                query($filter: ProjectFilter) {
                    projects(filter: $filter, first: 100) {
                        nodes { id name url externalLinks { nodes { url } } }
                        pageInfo { hasNextPage endCursor }
                    }
                }
            """
        variables: dict = {"filter": {"accessibleTeams": {"id": {"eq": team_id}}}}
        if cursor:
            variables["after"] = cursor
        data = linear_gql(api_key, query, variables)
        for node in data["projects"]["nodes"]:
            node["external_link_urls"] = {
                link["url"] for link in node.get("externalLinks", {}).get("nodes", [])
            }
            projects[node["name"]] = node
        page_info = data["projects"]["pageInfo"]
        has_more = page_info["hasNextPage"]
        cursor = page_info["endCursor"]
    return projects


def truncate_name(name: str) -> str:
    if len(name) <= MAX_PROJECT_NAME_LEN:
        return name
    return name[: MAX_PROJECT_NAME_LEN - 1] + "…"


def linear_add_project_link(api_key: str, project_id: str, url: str, label: str) -> None:
    data = linear_gql(api_key, """
        mutation($input: EntityExternalLinkCreateInput!) {
            entityExternalLinkCreate(input: $input) {
                success
            }
        }
    """, {"input": {"projectId": project_id, "url": url, "label": label}})
    if not data["entityExternalLinkCreate"]["success"]:
        raise RuntimeError(f"Failed to add link to project: {url}")


def linear_create_project(api_key: str, team_ids: list[str], name: str, description: str) -> dict:
    data = linear_gql(api_key, """
        mutation($input: ProjectCreateInput!) {
            projectCreate(input: $input) {
                success
                project { id name url }
            }
        }
    """, {"input": {"name": truncate_name(name), "teamIds": team_ids, "description": description}})
    result = data["projectCreate"]
    if not result["success"]:
        raise RuntimeError(f"Failed to create project: {name}")
    return result["project"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import GitHub issue trees into Linear as projects.",
    )
    parser.add_argument(
        "issues", nargs="+", type=int, metavar="ISSUE",
        help="GitHub issue numbers to import. Each issue and its transitive sub-issues become Linear projects.",
    )
    parser.add_argument("--repo", default="MaterializeInc/database-issues")
    parser.add_argument("--team", required=True, help="Linear team key (e.g. SQL)")
    parser.add_argument("--include-closed", action="store_true", help="Include closed issues")
    parser.add_argument("--patch-links", action="store_true",
                        help="Add GitHub URL as a resource link to existing projects that are missing it")
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

    # Flatten all trees into a single list of issues to create as projects.
    all_issues: list[GHIssue] = []
    for tree in trees:
        all_issues.extend(flatten(tree))

    if not args.include_closed:
        all_issues = [i for i in all_issues if i.state != "closed"]

    print(f"\nCrawl structure:")
    for tree in trees:
        print_tree(tree, indent=1)

    print(f"\nProjects to create ({len(all_issues)}):")
    for issue in all_issues:
        marker = "x" if issue.state == "closed" else " "
        name = truncate_name(issue.title)
        suffix = " [truncated]" if name != issue.title else ""
        print(f"  [{marker}] #{issue.number}: {name}{suffix}")

    if args.dry_run:
        print("\n[dry run] Nothing created.")
        return

    print()
    team = linear_find_team(api_key, args.team)
    print(f"Linear team: {team['name']} ({team['key']})")

    print("Fetching existing projects...")
    existing = linear_fetch_existing_projects(api_key, team["id"])
    print(f"Found {len(existing)} existing projects.\n")

    created = 0
    skipped = 0
    patched = 0
    for issue in all_issues:
        name = truncate_name(issue.title)
        if name in existing:
            proj = existing[name]
            if args.patch_links and issue.url not in proj.get("external_link_urls", set()):
                linear_add_project_link(api_key, proj["id"], issue.url, "GitHub Issue")
                proj["external_link_urls"].add(issue.url)
                print(f"  [link] {name}")
                print(f"    added {issue.url}")
                patched += 1
            else:
                print(f"  [skip] {name}")
            print(f"    -> {proj['url']}")
            skipped += 1
            continue
        description = f"Imported from GitHub: {issue.url}"
        project = linear_create_project(api_key, [team["id"]], issue.title, description)
        # Also add the GitHub link as a resource on newly created projects.
        linear_add_project_link(api_key, project["id"], issue.url, "GitHub Issue")
        project["external_link_urls"] = {issue.url}
        existing[name] = project
        print(f"  [new]  {name}")
        print(f"    -> {project['url']}")
        created += 1

    summary = [f"Created {created}"]
    if patched:
        summary.append(f"patched {patched}")
    summary.append(f"skipped {skipped - patched}")
    print(f"\n{', '.join(summary)}.")

    print("\nDone!")


if __name__ == "__main__":
    main()
