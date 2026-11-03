"""Build both CPU architectures and package the embedded editor with the server."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--os', required=True, choices=['darwin', 'linux', 'windows'])
args = parser.parse_args()
if not (root / 'web/dist/index.html').is_file():
    parser.error('Build the client first: cd ../client && npm ci && npm run build')
release = root / 'release'
release.mkdir(exist_ok=True)
server_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
client_commit = os.environ.get('CLIENT_COMMIT')
if not client_commit and (root.parent / 'client/.git').exists():
    client_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root.parent / 'client', text=True).strip()
for arch in ['amd64', 'arm64']:
    name = f'limulus-{args.os}-{arch}'
    with tempfile.TemporaryDirectory() as temp:
        package = Path(temp) / name
        package.mkdir()
        binary = package / ('limulus.exe' if args.os == 'windows' else 'limulus')
        env = dict(os.environ, CGO_ENABLED='0', GOOS=args.os, GOARCH=arch)
        subprocess.run(['go', 'build', '-trimpath', '-o', str(binary), '.'], cwd=root, env=env, check=True)
        binary.chmod(0o755)
        shutil.copy2(root / 'README.md', package)
        shutil.copytree(root / 'examples', package / 'examples')
        for license_file in root.glob('LICENSE*'):
            shutil.copy2(license_file, package)
        (package / 'BUILD.txt').write_text(
            f'Server: {server_commit}\nClient: {client_commit or "unknown (local bundle)"}\n'
            f'Target: {args.os}/{arch}\n', encoding='utf-8')
        archive = Path(shutil.make_archive(str(release / name),
                       'zip' if args.os == 'windows' else 'gztar', temp, name))
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    Path(str(archive) + '.sha256').write_text(f'{digest}  {archive.name}\n', encoding='utf-8')
    print(archive)
