@echo off
setlocal
echo.
echo ==========================================
echo OSP FRONTEND PATH + CACHE DIAGNOSIS
echo ==========================================
echo.

echo [1] page.tsx files in this project:
for /r %%F in (page.tsx) do echo %%F

echo.
echo [2] Files still containing the old Brenham default:
findstr /s /n /i /m "Brenham Weekly Redline Review" *.tsx *.ts *.jsx *.js

echo.
echo [3] Files still containing old Ready fallback:
findstr /s /n /i /m "\"Ready\"" *.tsx *.ts *.jsx *.js

echo.
echo [4] Removing .next cache if it exists...
if exist .next (
  rmdir /s /q .next
  echo .next deleted.
) else (
  echo .next not found.
)

echo.
echo Done.
echo Now restart the frontend with: npm run dev
echo Then hard refresh the browser with Ctrl+F5
echo.
pause
