import dulwich.objects
import dulwich.repo
from pathlib import Path
import stat
from typing import Iterable, Optional, Tuple

from instiki2git.percent_code import PercentCode


def mode(obj: dulwich.objects.ShaFile) -> int:
    if isinstance(obj, dulwich.objects.Tree):
        return stat.S_IFDIR
    if isinstance(obj, dulwich.objects.Blob):
        return stat.S_IFREG | 0o644
    raise TypeError('unexpected object')

def path(p: Path) -> list[bytes]:
    return [part.encode('utf8') for part in p.parts]

def get_commit(repo: dulwich.repo.Repo, id: bytes) -> dulwich.repo.Commit:
    obj: dulwich.objects.ShaFile = repo.get_object(id)
    if not isinstance(obj, dulwich.repo.Commit):
        raise TypeError('does not refer to a commit: {sha.decode()}')
    return obj

def add_blob(repo: dulwich.repo.Repo, content: bytes) -> dulwich.objects.Blob:
    blob = dulwich.objects.Blob()
    blob._set_data(content)
    repo.object_store.add_object(blob)
    return blob

def add_tree(repo: dulwich.repo.Repo, entries: Iterable[Tuple[bytes, dulwich.objects.ShaFile]]) -> dulwich.objects.Tree:
    tree = dulwich.objects.Tree()
    for (name, obj) in entries:
        tree[name] = (mode(obj), obj.id)
    repo.object_store.add_object(tree)
    return tree

def add_flat_tree(repo: dulwich.repo.Repo, files: list[Tuple[bytes, bytes]]) -> dulwich.objects.Tree:
    return add_tree(repo, ((name, add_blob(repo, content)) for (name, content) in files))

def tree_get(
    repo: dulwich.repo.Repo,
    tree: dulwich.objects.Tree,
    name: bytes,
) -> Optional[dulwich.objects.ShaFile]:
    """Get an entry from a tree, if it exists."""
    if not name in tree:
        return None
    (_, id) = tree[name]
    return repo.get_object(id)

def tree_set(
    repo: dulwich.repo.Repo,
    tree: dulwich.objects.Tree,
    name: bytes,
    obj: dulwich.objects.ShaFile,
) -> dulwich.objects.Tree:
    """
    Set an entry in a tree.
    The new tree is added to the object store of the given repository.
    Returns the new tree.
    """
    tree[name] = (mode(obj), obj.id)
    repo.object_store.add_object(tree)
    return tree

def tree_replace_nested(
    repo: dulwich.repo.Repo,
    tree: Optional[dulwich.objects.ShaFile],
    path: list[bytes],
    obj: dulwich.objects.ShaFile,
) -> dulwich.objects.ShaFile:
    """
    Replace a nested entry in a nested tree.
    If no nested tree is given, it is freshly created.
    All objects created during this are added to the object store of the repository.
    """
    def f(tree: Optional[dulwich.objects.Tree], i: int):
        if i == len(path):
            return obj

        # Now we assume that tree has type Optional[dulwich.objects.Tree].
        if tree is None:
            tree = dulwich.objects.Tree()

        name = path[i]
        sub = tree_get(repo, tree, name)
        sub = f(sub, i + 1)
        return tree_set(repo, tree, name, sub)

    return f(tree, 0)

# These are the reserved bytes in git commit authors and committers.
identity_code = PercentCode(reserved = map(ord, ['\0', '\n', '<', '>']))

def identity_with_empty_email(name: bytes) -> bytes:
    """Format a git user identity without an email address."""
    return name + b' <>'


#r = dulwich.repo.Repo('/home/noname/git-test')
#add_blob(r, b'123')
#add_flat_tree(r, [(b'a', b'123'), (b'b', b'qwe')])
