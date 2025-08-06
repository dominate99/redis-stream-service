import { xadd, xrange, xlen, xread } from './index.js';



describe('Backend Integration Tests', () => {
    // This test suite requires the backend server to be running.
    // Make sure to start it with `python backend/app.py` before running these tests.

    it('should perform a full XADD, XLEN, XRANGE, and XREAD cycle on a single stream', async () => {
        // 1. Check initial length (should be 0 for a new stream)

        // Use a unique stream name for each test run to avoid conflicts
        const STREAM_NAME = `test-stream-${Date.now()}`;

        const initialLen = await xlen(STREAM_NAME);
        expect(initialLen.length).toBe(0);

        // 2. Add some entries to the stream
        const entry1 = { rider: 'Castilla', speed: 30.2, position: 1, location_id: 1 };
        const entry2 = { rider: 'Norem', speed: 28.8, position: 3, location_id: 1 };
        const entry3 = { rider: 'Prickett', speed: 29.7, position: 2, location_id: 1 };
        const addResult1 = await xadd(STREAM_NAME, entry1);
        const addResult2 = await xadd(STREAM_NAME, entry2);
        const addResult3 = await xadd(STREAM_NAME, entry3);

        // Assert that adding entries returns valid IDs
        expect(addResult1.id).toBeDefined();
        expect(addResult2.id).toBeDefined();
        expect(addResult3.id).toBeDefined();

        // 3. Check the new length
        const newLen = await xlen(STREAM_NAME);
        expect(newLen.length).toBe(3);

        // 4. Use XRANGE to read the entries back
        const rangeEntries = await xrange(STREAM_NAME, 10);
        expect(rangeEntries).toHaveLength(3);
        expect(rangeEntries[0].fields).toEqual(entry1);
        expect(rangeEntries[1].fields).toEqual(entry2);
        expect(rangeEntries[2].fields).toEqual(entry3);

        // 5. Use XREAD to read entries after the first one
        const firstId = rangeEntries[0].id;
        const readEntries = await xread([STREAM_NAME], [firstId]);
        expect(readEntries[0][1]).toHaveLength(2);
        expect(readEntries[0][1][0].fields).toEqual(entry2);
        expect(readEntries[0][1][1].fields).toEqual(entry3);
    });

    it('should handle XREAD on multiple streams and read new entries after XADD', async () => {
        const STREAM_A = `multi-stream-a-${Date.now()}`;
        const STREAM_B = `multi-stream-b-${Date.now()}`;

        // 1. Add initial entries to two different streams
        const entryA1 = { stream: 'A', value: 1 };
        const entryB1 = { stream: 'B', value: 1 };
        await xadd(STREAM_A, entryA1);
        await xadd(STREAM_B, entryB1);

        // 2. Perform an initial XREAD on both streams from the beginning
        let readResult = await xread([STREAM_A, STREAM_B], ['0-0', '0-0']);

        // The order of streams in the result is not guaranteed, so we sort for consistency
        readResult.sort((a, b) => a[0].localeCompare(b[0]));

        expect(readResult).toHaveLength(2);
        expect(readResult[0][0]).toBe(STREAM_A);
        expect(readResult[0][1][0].fields).toEqual(entryA1);
        expect(readResult[1][0]).toBe(STREAM_B);
        expect(readResult[1][1][0].fields).toEqual(entryB1);

        // 3. Get the last IDs from the initial read
        const lastIdA = readResult[0][1][0].id;
        const lastIdB = readResult[1][1][0].id;

        // 4. Add a new entry only to STREAM_A
        const entryA2 = { stream: 'A', value: 2 };
        await xadd(STREAM_A, entryA2);

        // 5. XREAD again, using the last known IDs. We expect to get only the new entry from STREAM_A.
        const newReadResult = await xread([STREAM_A, STREAM_B], [lastIdA, lastIdB]);
        expect(newReadResult).toHaveLength(1);
        expect(newReadResult[0][0]).toBe(STREAM_A);
        expect(newReadResult[0][1]).toHaveLength(1);
        expect(newReadResult[0][1][0].fields).toEqual(entryA2);
    });
});
