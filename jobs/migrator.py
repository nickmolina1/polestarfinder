def handler(event, context):
    from database.migrate import main
    main()
    return {"ok": True}