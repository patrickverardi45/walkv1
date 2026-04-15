# OSP Redlining Agent Guide

## Rules
- Diagnose first
- Do not guess
- Do not redesign
- Make minimal targeted changes
- Return full file replacements only
- Always include backend and frontend reload commands after every file output

## Project structure
- backend = FastAPI backend
- web = frontend app

## Standard run commands

Backend terminal:
uvicorn main:app --reload

Frontend terminal:
npm run dev

## Behavior
- Preserve working features
- Do not touch unrelated files
- Preserve map behavior, station anchoring, hitbox alignment, and photo upload functionality
- Prefer surgical fixes over rewrites