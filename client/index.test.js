import { xadd, xrange, xlen, xread } from './index.js';
import fetch from 'node-fetch';

jest.mock('node-fetch');

const BACKEND_URL = 'http://localhost:5001';

describe('Client functions', () => {
    afterEach(() => {
        fetch.mockClear();
    });

    it('xadd should call the correct endpoint and return data', async () => {
        fetch.mockImplementation(() => Promise.resolve({ json: () => Promise.resolve({ id: '123' }) }));
        const result = await xadd('test_stream', { key: 'value' });
        expect(fetch).toHaveBeenCalledWith(`${BACKEND_URL}/xadd/test_stream`, expect.anything());
        expect(result).toEqual({ id: '123' });
    });

    it('xrange should call the correct endpoint and return data', async () => {
        fetch.mockImplementation(() => Promise.resolve({ json: () => Promise.resolve([{ id: '123' }]) }));
        const result = await xrange('test_stream', 10);
        expect(fetch).toHaveBeenCalledWith(`${BACKEND_URL}/xrange/test_stream?count=10`);
        expect(result).toEqual([{ id: '123' }]);
    });

    it('xlen should call the correct endpoint and return data', async () => {
        fetch.mockImplementation(() => Promise.resolve({ json: () => Promise.resolve({ length: 5 }) }));
        const result = await xlen('test_stream');
        expect(fetch).toHaveBeenCalledWith(`${BACKEND_URL}/xlen/test_stream`);
        expect(result).toEqual({ length: 5 });
    });

    it('xread should call the correct endpoint and return data', async () => {
        fetch.mockImplementation(() => Promise.resolve({ json: () => Promise.resolve([['test_stream', [{ id: '123' }]]]) }));
        const result = await xread(['test_stream'], ['0-0'], 10);
        expect(fetch).toHaveBeenCalledWith(`${BACKEND_URL}/xread?streams=test_stream 0-0&count=10`);
        expect(result).toEqual([['test_stream', [{ id: '123' }]]]);
    });

    it('xread should handle multiple streams', async () => {
        fetch.mockImplementation(() => Promise.resolve({ json: () => Promise.resolve([['stream1', [{ id: '123' }]], ['stream2', [{ id: '456' }]]]) }));
        const result = await xread(['stream1', 'stream2'], ['0-0', '0-0'], 10);
        expect(fetch).toHaveBeenCalledWith(`${BACKEND_URL}/xread?streams=stream1 0-0 stream2 0-0&count=10`);
        expect(result).toEqual([['stream1', [{ id: '123' }]], ['stream2', [{ id: '456' }]]]);
    });

    it('xread should handle $ parameter', async () => {
        fetch.mockImplementation(() => Promise.resolve({ json: () => Promise.resolve([['test_stream', [{ id: '123' }]]]) }));
        const result = await xread(['test_stream'], ['$'], 10);
        expect(fetch).toHaveBeenCalledWith(`${BACKEND_URL}/xread?streams=test_stream $&count=10`);
        expect(result).toEqual([['test_stream', [{ id: '123' }]]]);
    });

    it('xadd should throw an error for a non-ok response', async () => {
        fetch.mockImplementation(() => Promise.resolve({
            ok: false,
            status: 500,
        }));

        // Use rejects.toThrow for async errors
        await expect(xadd('test_stream', { key: 'value' })).rejects.toThrow('HTTP error! status: 500');
    });
});