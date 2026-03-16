# Push to GitHub

## 1. Create the repo on GitHub

1. Go to [github.com/new](https://github.com/new)
2. Repository name: `CardChamp` (or your choice)
3. Choose **Private** (recommended – contains integration logic)
4. **Do not** initialize with README, .gitignore, or license
5. Click **Create repository**

## 2. Add remote and push

Replace `YOUR_USERNAME` with your GitHub username:

```bash
cd /Users/user/Documents/CardChamp

git remote add origin https://github.com/YOUR_USERNAME/CardChamp.git
git branch -M main
git push -u origin main
```

Or if you use SSH:

```bash
git remote add origin git@github.com:YOUR_USERNAME/CardChamp.git
git branch -M main
git push -u origin main
```

## 3. Security note

`.env` is in `.gitignore` and will **not** be pushed. Add your credentials locally after cloning on a new machine.
